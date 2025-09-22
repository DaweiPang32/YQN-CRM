# app.py —— CRM（客户流程 + 六阶段备注，多表，渠道、沉睡、勾选完成、看板式展示、URL跳转）
# 依赖：streamlit, pandas, gspread, google-auth
# 运行：streamlit run app.py

import streamlit as st
import pandas as pd
import gspread
import requests
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound, APIError
from datetime import datetime
from zoneinfo import ZoneInfo
import uuid
import re
import os
import time

# ========== 基本配置 ==========
st.set_page_config(page_title="客户信息小程序", page_icon="📇", layout="wide")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
TZ = ZoneInfo("America/Los_Angeles")

# 6 个阶段（按顺序推进，英文）
PIPELINE_STEPS = ["TouchBase", "Qualify", "Propose", "Develop", "Close", "Fulfill"]
SLEEPING_STATUS = "沉睡"   # 旁路状态（暂停沟通）

# 根据阶段自动生成时间戳列名
STATUS_TS_COLS = [f"{s}_时间" for s in PIPELINE_STEPS]

# 主表列（新增：渠道）
COLUMNS = [
    "customer_id",
    "Company Name",
    "Address",
    "Contact",
    "客户邮箱",         # ✅ 新增
    "业务",
    "Preferred WHS Location",
    "渠道",
    "客户需求",         # ✅ 新增
    "销售信息",         # ✅ 新增（备注报价、折扣、跟进要点等）
    "当前状态",
    *STATUS_TS_COLS,    # TouchBase_时间 ... Fulfill_时间
    "销售",
]


# 备注表（每阶段一张）——后缀中文“代办任务”；新增「完成」列（"是"/""）
NOTES_SHEETS = {s: f"{s}代办任务" for s in PIPELINE_STEPS}
NOTE_COLUMNS = ["note_id", "customer_id", "内容", "创建时间", "完成"]

# ========== Session State ==========
if "selected_customer_id" not in st.session_state:
    st.session_state.selected_customer_id = ""
if "selected_customer_name" not in st.session_state:
    st.session_state.selected_customer_name = ""
if "_sheet_id_cached" not in st.session_state:
    st.session_state["_sheet_id_cached"] = ""

# ========== 小工具 ==========
def _col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def _as_tz_aware(dt):
    """把 datetime 统一转为带 LA 时区；遇到 None/NaT 直接返回 None。"""
    if dt is None:
        return None
    try:
        import pandas as pd
        if pd.isna(dt):   # 捕捉 NaT/NaN
            return None
    except Exception:
        pass
    tzinfo = getattr(dt, "tzinfo", None)
    if tzinfo is None:
        return dt.replace(tzinfo=TZ)
    return dt.astimezone(TZ)


def now_str() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")

def gen_customer_id() -> str:
    return uuid.uuid4().hex[:8].upper()

def validate_phone_or_email(contact: str) -> bool:
    if not contact:
        return False
    has_phone = re.search(r"\+?\d[\d\s\-]{6,}", contact or "") is not None
    has_email = re.search(r"[^@\s]+@[^@\s]+\.[^@\s]+", contact or "") is not None
    has_name = len(str(contact).strip()) >= 2
    return has_phone or has_email or has_name

def allowed_next_steps(current: str):
    """流水线仅支持前进一步；沉睡不在流水线中（单独按钮处理）。"""
    if current == SLEEPING_STATUS:
        # 沉睡状态下不显示前进按钮（需先唤醒）
        return []
    if current not in PIPELINE_STEPS:
        return [PIPELINE_STEPS[0]]
    idx = PIPELINE_STEPS.index(current)
    if idx == len(PIPELINE_STEPS) - 1:
        return []
    return PIPELINE_STEPS[idx + 1: idx + 2]

def _parse_dt_flex(s: str):
    s = str(s or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d"):
        try:
            dt = datetime.strptime(s, fmt)
            return _as_tz_aware(dt)  # 统一为带时区
        except:
            pass
    return None

def sh_cache_key(_sh) -> str:
    try:
        return getattr(_sh, "id", None) or _sh.id
    except Exception:
        return "spreadsheet-unknown"

def ws_cache_key(_ws) -> str:
    try:
        wid = getattr(_ws, "id", None) or _ws._properties.get("sheetId")
        sid = _ws.spreadsheet.id
        return f"{sid}:{wid}:{_ws.title}"
    except Exception:
        return f"ws:{getattr(_ws, 'title', 'unknown')}"

# --- 兼容不同版本 Streamlit 的 URL 参数与重载 ---
def _get_query_params() -> dict:
    try:
        # 新版 Streamlit
        return dict(st.query_params)
    except Exception:
        try:
            return st.experimental_get_query_params()
        except Exception:
            return {}

def _set_query_params(params: dict):
    norm = {k: ("" if v is None else str(v)) for k, v in params.items()}
    try:
        # 新版
        st.query_params.clear()
        for k, v in norm.items():
            st.query_params[k] = v
    except Exception:
        try:
            st.experimental_set_query_params(**norm)
        except Exception:
            pass

def _rerun():
    if hasattr(st, "experimental_rerun"):
        st.experimental_rerun()
    else:
        st.rerun()

# ===== 限流与重试封装 =====
def _backoff_sleep(i, base=0.8, cap=6.0):
    time.sleep(min(cap, base * (2 ** i)))

def safe_get_all_values(_ws, max_retries=5):
    last_err = None
    for i in range(max_retries):
        try:
            return _ws.get_all_values()
        except APIError as e:
            if "429" in str(e) or "Quota exceeded" in str(e) or "limit" in str(e):
                _backoff_sleep(i); last_err = e; continue
            raise
    if last_err: raise last_err

def _safe_append_row(_ws, row, max_retries=5):
    last_err = None
    for i in range(max_retries):
        try:
            _ws.append_row(row, value_input_option="USER_ENTERED"); return True
        except APIError as e:
            if "429" in str(e) or "Quota exceeded" in str(e) or "limit" in str(e):
                _backoff_sleep(i); last_err = e; continue
            raise
    if last_err: raise last_err

def safe_values_batch_get(_sh, ranges, max_retries=5):
    """Use gspread.batch_get to fetch multiple ranges with retries."""
    last_err = None
    for i in range(max_retries):
        try:
            return _sh.batch_get(ranges)
        except APIError as e:
            if "429" in str(e) or "Quota exceeded" in str(e) or "limit" in str(e):
                _backoff_sleep(i); last_err = e; continue
            raise
    if last_err: raise last_err

# ========== Google 连接 ==========
@st.cache_resource(show_spinner=False, ttl=600)
def get_ws_map_cached(_sh):
    """返回 {title -> Worksheet 对象} 的映射；只触发一次 metadata 读取。"""
    ws_list = _sh.worksheets()
    return {w.title.strip(): w for w in ws_list}

@st.cache_resource(show_spinner=False)
def get_gspread_client():
    if "gcp_service_account" in st.secrets:
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)
        return gspread.authorize(creds)
    json_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    if json_path and os.path.exists(json_path):
        creds = Credentials.from_service_account_file(json_path, scopes=SCOPES)
        return gspread.authorize(creds)
    default_json = os.path.join(os.getcwd(), "service_account.json")
    if os.path.exists(default_json):
        creds = Credentials.from_service_account_file(default_json, scopes=SCOPES)
        return gspread.authorize(creds)
    st.error("未找到谷歌凭证：请配置 service account。"); st.stop()

def _sheet_name() -> str:
    return st.secrets.get("sheets", {}).get("SHEET_NAME", "客户信息")

def _sheet_id_from_secrets_or_session() -> str:
    return st.secrets.get("sheets", {}).get("SHEET_ID", "") or st.session_state.get("_sheet_id_cached", "")

def safe_open_spreadsheet(_gc, sheet_id: str | None, title: str, max_retries: int = 6):
    last = None
    for i in range(max_retries):
        try:
            return _gc.open_by_key(sheet_id) if sheet_id else _gc.open(title)
        except APIError as e:
            if "429" in str(e) or "Quota exceeded" in str(e) or "limit" in str(e):
                _backoff_sleep(i); last = e; continue
            raise
    if last: raise last

def safe_get_or_create_worksheet(_sh, title: str, ncols: int, max_retries: int = 6):
    last = None
    for i in range(max_retries):
        try:
            try:
                ws = _sh.worksheet(title)
            except WorksheetNotFound:
                ws = _sh.add_worksheet(title=title, rows="2000", cols=str(ncols))
            return ws
        except APIError as e:
            if "429" in str(e) or "Quota exceeded" in str(e) or "limit" in str(e):
                _backoff_sleep(i); last = e; continue
            raise
    if last: raise last

def ensure_header(_ws, columns):
    """仅在必要时最少读一次，确保表头与期望一致。"""
    try:
        data = safe_get_all_values(_ws)
    except APIError:
        data = []
    if not data:
        _ws.append_row(columns); return
    if data[0] != columns:
        end_col = _col_letter(len(columns))
        _ws.update(f"A1:{end_col}1", [columns])

@st.cache_resource(show_spinner=False, ttl=600)
def get_main_handles_cached(sheet_id_key: str, sheet_title_key: str):
    _gc = get_gspread_client()
    sh = safe_open_spreadsheet(_gc, sheet_id_key or None, sheet_title_key)
    try: st.session_state["_sheet_id_cached"] = sh.id
    except: pass
    ws = safe_get_or_create_worksheet(sh, "Sheet1", len(COLUMNS))
    ensure_header(ws, COLUMNS)
    return sh, ws

# ========== 读写 ==========
@st.cache_data(show_spinner=False, ttl=60)
def read_df_cached(_ws, cache_key: str) -> pd.DataFrame:
    vals = safe_get_all_values(_ws)
    if not vals:
        return pd.DataFrame(columns=COLUMNS)
    df = pd.DataFrame(vals[1:], columns=vals[0]) if len(vals) > 1 else pd.DataFrame(columns=vals[0])

    if df.empty:
        df = pd.DataFrame(columns=vals[0])

    # 仅保留有 ID 的行
    if "customer_id" in df.columns:
        df = df[df["customer_id"].astype(str).str.len() > 0]

    # ✅ 兼容老表：缺失的列全部补空
    for col in COLUMNS:
        if col not in df.columns:
            df[col] = ""

    # 丢弃多余列（可选）——如不想丢，可注释掉
    df = df[COLUMNS]

    return df


def write_new_row(_ws, row_values: list):
    _safe_append_row(_ws, row_values)

def update_row(_ws, row_index: int, row_values: list, columns, max_retries: int = 5):
    end_col = _col_letter(len(columns))
    rng = f"A{row_index+2}:{end_col}{row_index+2}"
    last_err = None
    for i in range(max_retries):
        try:
            _ws.update(rng, [row_values])
            return
        except APIError as e:
            if "429" in str(e) or "Quota exceeded" in str(e) or "limit" in str(e):
                _backoff_sleep(i); last_err = e; continue
            raise
    if last_err: raise last_err

# ========== 备注子表 ==========
def _get_or_create_notes_ws(_gc, _sh, status: str):
    if status not in NOTES_SHEETS:
        raise ValueError(f"未知状态：{status}")
    expected = NOTES_SHEETS[status].strip()

    # 先从缓存拿，避免重复 metadata 读取
    ws_map = get_ws_map_cached(_sh)
    ws = ws_map.get(expected)

    # 未找到再创建（只写不读；写成功后刷新缓存一次）
    if ws is None:
        ws = _sh.add_worksheet(title=expected, rows="2000", cols=str(len(NOTE_COLUMNS)))
        ensure_header(ws, NOTE_COLUMNS)
        try:
            get_ws_map_cached.clear()
            get_ws_map_cached(_sh)
        except Exception:
            pass
        return ws

    # 已存在，确保表头
    ensure_header(ws, NOTE_COLUMNS)
    return ws

@st.cache_data(show_spinner=False, ttl=60)
def read_notes_df(_ws, cache_key: str) -> pd.DataFrame:
    vals = safe_get_all_values(_ws)
    if not vals: return pd.DataFrame(columns=NOTE_COLUMNS)
    df = pd.DataFrame(vals[1:], columns=vals[0])
    if df.empty:
        return pd.DataFrame(columns=NOTE_COLUMNS)
    # 兼容老表：缺失列补上
    for c in NOTE_COLUMNS:
        if c not in df.columns:
            df[c] = ""
    return df

def add_note(_ws, customer_id: str, content: str):
    # 新增时默认“未完成”（完成列留空）
    _safe_append_row(_ws, [uuid.uuid4().hex[:8].upper(), customer_id, (content or "").strip(), now_str(), ""])

def set_note_done(_ws, header_columns, row_zero_based: int, done: bool, max_retries: int = 5):
    """
    把第 row_zero_based（DataFrame 的 index，对应数据区 0-based，表中第 row_zero_based+2 行）
    的「完成」更新为 '是' 或 ''。
    """
    try:
        col_idx = header_columns.index("完成") + 1  # 1-based
    except ValueError:
        ensure_header(_ws, NOTE_COLUMNS)
        col_idx = NOTE_COLUMNS.index("完成") + 1

    cell = f"{_col_letter(col_idx)}{row_zero_based + 2}"
    payload = [["是" if done else ""]]

    last_err = None
    for i in range(max_retries):
        try:
            _ws.update(cell, payload, value_input_option="USER_ENTERED")
            return True
        except APIError as e:
            if "429" in str(e) or "Quota exceeded" in str(e) or "limit" in str(e):
                _backoff_sleep(i); last_err = e; continue
            raise
    if last_err:
        raise last_err

# ========== 最近更新时间聚合（用于“查看客户”页）==========
@st.cache_data(show_spinner=False, ttl=60)
def build_latest_note_dt_map(_gc, _sh, cache_key: str) -> dict:
    note_map = {}
    ranges = [f"'{title}'!A1:D" for title in NOTES_SHEETS.values()]  # D 列截止到“创建时间”
    res = None
    try:
        res = safe_values_batch_get(_sh, ranges)
    except Exception:
        res = None

    if res:
        for values in res:
            if not values or len(values) < 2:
                continue
            header = values[0]
            def idx(col_name, default=None):
                try: return header.index(col_name)
                except ValueError: return default
            idx_cid = idx("customer_id", 1); idx_ctim = idx("创建时间", 3)
            for r in values[1:]:
                cid = r[idx_cid] if idx_cid is not None and idx_cid < len(r) else ""
                if not cid: continue
                t = _parse_dt_flex(r[idx_ctim]) if idx_ctim is not None and idx_ctim < len(r) else None
                if not t: continue
                prev = note_map.get(cid)
                if (prev is None) or (t > prev): note_map[cid] = t
        return note_map

    # 降级逐表
    for status in PIPELINE_STEPS:
        ws = _get_or_create_notes_ws(_gc, _sh, status)
        df = read_notes_df(ws, ws_cache_key(ws))
        if df.empty: continue
        for _, r in df.iterrows():
            cid = str(r.get("customer_id", "")).strip()
            if not cid: continue
            t = _parse_dt_flex(r.get("创建时间", ""))
            if not t: continue
            prev = note_map.get(cid)
            if (prev is None) or (t > prev): note_map[cid] = t
    return note_map

def latest_activity_str_for_row(row, note_map: dict) -> str:
    dts = []
    for c in STATUS_TS_COLS:
        dt = _parse_dt_flex(row.get(c, "")); 
        if dt: dts.append(dt)
    cid = str(row.get("customer_id", "")).strip()
    if cid in note_map: dts.append(note_map[cid])
    return _as_tz_aware(max(dts)).strftime("%Y-%m-%d %H:%M:%S") if dts else ""

# ========== 打开表 ==========
try:
    gc = get_gspread_client()
    sheet_title = _sheet_name()
    sheet_id    = _sheet_id_from_secrets_or_session()
    sh, ws = get_main_handles_cached(sheet_id, sheet_title)
except APIError:
    st.error("Google Sheets 读配额暂时超限。稍后再试或减少并发。"); st.stop()

df = read_df_cached(ws, ws_cache_key(ws))

# ========= 导航：首次进入仅在“无 tab 参数”时默认 view；否则尊重 URL =========
params = _get_query_params()

def _first_val(v):
    if isinstance(v, list):
        return v[0] if v else ""
    return v or ""

url_tab = _first_val(params.get("tab"))
url_cid = _first_val(params.get("cid"))

# 1) 首次进入：只有当 URL 没有 tab 时，才默认到 view；否则按 URL 来
if "_visited_once" not in st.session_state:
    st.session_state["_visited_once"] = True
    if not url_tab:  # 没有任何 tab 参数 -> 统一到 view
        _set_query_params({"tab": "view", "cid": ""})
        desired_tab = "view"
    else:
        desired_tab = url_tab  # URL 已明确指定（如 progress），不要覆盖
else:
    # 非首次：始终以 URL 为准；若没有则回退 view
    desired_tab = url_tab or "view"

# 同步 cid（便于直接进详情）
if url_cid:
    st.session_state.selected_customer_id = url_cid

# 2) 定义导航项（顺序可调）
NAV = {"view": "📋 查看客户", "progress": "⏩ 推进状态 & 添加备注", "new": "➕ 添加客户"}
nav_keys = list(NAV.keys())

# 3) URL tab 变化时，换一个 widget key，强制应用 index（避免 radio 记忆旧选项）
if "_nav_widget_ver" not in st.session_state:
    st.session_state["_nav_widget_ver"] = 0
if "_nav_desired_last" not in st.session_state:
    st.session_state["_nav_desired_last"] = None
if desired_tab != st.session_state["_nav_desired_last"]:
    st.session_state["_nav_widget_ver"] += 1
    st.session_state["_nav_desired_last"] = desired_tab

nav_widget_key = f"__nav_radio__v{st.session_state['_nav_widget_ver']}"

# 4) 计算 index 并渲染 radio（只渲染这一处导航；删除其它重复的 radio）
try:
    default_index = nav_keys.index(desired_tab)
except ValueError:
    default_index = 0

nav = st.radio(
    "页面导航",
    options=nav_keys,
    format_func=lambda k: NAV[k],
    horizontal=True,
    index=default_index,
    key=nav_widget_key,
)

# 5) 用户手动切换时，回写 URL（进入 progress 才带 cid）
if nav != desired_tab:
    _set_query_params({
        "tab": nav,
        "cid": st.session_state.get("selected_customer_id", "") if nav == "progress" else ""
    })
    _rerun()


# ========== 自定义“伪 Tab”导航（可编程切换） ==========
def _goto(tab_key: str, cid: str = ""):
    if cid:
        st.session_state.selected_customer_id = cid
        if not df.empty:
            m = df[df["customer_id"] == cid]
            if not m.empty:
                st.session_state.selected_customer_name = m.iloc[0]["Company Name"]
    _set_query_params({"tab": tab_key, "cid": st.session_state.selected_customer_id if tab_key=="progress" else ""})
    _rerun()


# ======================= 页面：📋 查看客户 =======================
if nav == "view":
    st.header("📋 查看客户")
    if st.button("🔄 刷新数据", use_container_width=True):
        st.cache_data.clear(); _rerun()

    if df.empty:
        st.info("暂无客户数据。请切换到【➕ 添加客户】。")
    else:
        # 最近更新时间（含“备注创建时间”与状态时间）
        note_dt_map = build_latest_note_dt_map(gc, sh, sh_cache_key(sh))
        view_df = df.copy()
        view_df["最近更新时间"] = view_df.apply(lambda r: latest_activity_str_for_row(r, note_dt_map), axis=1)
        view_df["是否完成"] = view_df["当前状态"].apply(lambda s: "是" if s == "Fulfill" else "否")

        # 最近推进时间（仅看状态时间戳，不含备注）
        def _latest_progress_dt(row):
            dts = []
            for c in STATUS_TS_COLS:
                dt = _parse_dt_flex(row.get(c, ""))
                if dt: dts.append(dt)
            return max(dts) if dts else None

        now_dt = datetime.now(TZ)
        view_df["最近推进时间"] = view_df.apply(_latest_progress_dt, axis=1)

        def _days_since_safe(t, status):
            # 沉睡：不预警
            if status == SLEEPING_STATUS:
                return None
            # None/NaT：不计算
            try:
                import pandas as pd
                if t is None or pd.isna(t):
                    return None
            except Exception:
                if t is None:
                    return None
            aware = _as_tz_aware(t)
            if aware is None:
                return None
            return (now_dt.date() - aware.date()).days

        view_df["距上次推进_天"] = view_df.apply(
            lambda r: _days_since_safe(r.get("最近推进时间"), r.get("当前状态")), axis=1
        )


        # 过滤区（按销售 + 渠道 + 状态；默认不显示沉睡）
        c1, c2, c3, c4, c5 = st.columns([2, 1, 1, 1, 1])
        with c1:
            kw = st.text_input("关键词（公司名/ID/联系人）")
        with c2:
            only_open = st.checkbox("只看未完成", value=False)
        with c3:
            status_sel = st.multiselect(
                "状态（默认隐藏沉睡）",
                [SLEEPING_STATUS] + PIPELINE_STEPS
            )
        with c4:
            sales_options = sorted([s for s in view_df.get("销售", pd.Series(dtype=str)).dropna().unique() if str(s).strip() != ""])
            sales_sel = st.multiselect("销售", options=sales_options)
        with c5:
            channel_options = sorted([s for s in view_df.get("渠道", pd.Series(dtype=str)).dropna().unique() if str(s).strip() != ""])
            channel_sel = st.multiselect("渠道", options=channel_options, help="如：销售自拓 / 客户referral 等")

        show = view_df.copy()

        # 关键词过滤
        if kw.strip():
            lk = kw.strip().lower()
            show = show[
                show["Company Name"].fillna("").str.lower().str.contains(lk) |
                show["customer_id"].fillna("").str.lower().str.contains(lk) |
                show["Contact"].fillna("").str.lower().str.contains(lk)
            ]

        # ✅ 状态过滤：若未选择任何状态 -> 默认排除沉睡
        if status_sel and len(status_sel) > 0:
            show = show[show["当前状态"].isin(status_sel)]
        else:
            show = show[show["当前状态"] != SLEEPING_STATUS]

        # 只看未完成（不影响上面的“默认隐藏沉睡”）
        if only_open:
            show = show[show["是否完成"] != "是"]

        # 销售 / 渠道过滤
        if sales_sel:
            show = show[show["销售"].isin(sales_sel)]
        if channel_sel:
            show = show[show["渠道"].isin(channel_sel)]


        # 组装表格数据（沉睡客户的“距上次推进_天”为空）
        table = show[[
            "Company Name", "customer_id", "销售", "渠道", "当前状态",
            "最近更新时间", "最近推进时间", "距上次推进_天", "是否完成"
        ]].copy()

        # 打开链接
        table.insert(0, "查看", table["customer_id"].apply(lambda x: f"?tab=progress&cid={x}"))

        # 最近推进时间格式化（避免 tz-naive）
        def _fmt_dt_safe(t):
            try:
                import pandas as pd
                if t is None or pd.isna(t):
                    return "（无）"
            except Exception:
                if t is None:
                    return "（无）"
            tt = _as_tz_aware(t)
            return tt.strftime("%Y-%m-%d %H:%M:%S") if tt else "（无）"

        table["最近推进时间_显示"] = table["最近推进时间"].apply(
            lambda t: _fmt_dt_safe(t)  # 你前面第(3)步补过的安全格式化函数
        )
        
        # 彩色徽标（沉睡不显示）
        def color_badge(days, status):
            if status == SLEEPING_STATUS:
                return ""  # 沉睡不预警
            if days is None or days == "":
                return ""
            try:
                d = int(days)
            except:
                return ""
            if d <= 6:
                return f"🟢 {d}"
            elif d <= 15:
                return f"🟠 {d}"
            else:
                return f"🔴 {d}"

        table["距上次推进_天_显示"] = table.apply(
            lambda r: color_badge(r.get("距上次推进_天"), r.get("当前状态")), axis=1
        )
        # 默认按“距上次推进_天”降序排列（沉睡或空值在最后）
        table = table.sort_values("距上次推进_天", ascending=False, na_position="last").reset_index(drop=True)

        # 展示列：显示“红点+数字”的文本列 & 保留原始数字列以便排序/筛选
        display_cols = [
            "查看", "Company Name",  "销售", "渠道", "当前状态",
            "最近推进时间_显示", "距上次推进_天_显示", 
        ]

        st.data_editor(
            table[display_cols],
            key="view_table",
            num_rows="fixed",
            use_container_width=True,
            height=560,
            hide_index=True,
            column_config={
                "查看": st.column_config.LinkColumn("查看", display_text="打开"),
                "最近推进时间_显示": st.column_config.TextColumn("最近推进时间"),
                "距上次推进_天": st.column_config.NumberColumn("距上次推进_天（数值）", help="与今天相差天数（可排序/筛选；沉睡客户为空）"),
                "距上次推进_天_显示": st.column_config.TextColumn("距上次推进_天", help="红点为视觉提醒（沉睡客户不显示）"),
            },
            disabled=["查看","Company Name","customer_id","销售","渠道","当前状态","最近更新时间","最近推进时间_显示","距上次推进_天","距上次推进_天_显示","是否完成"],
        )

# ======================= 页面：➕ 添加客户 =======================
elif nav == "new":
    st.header("➕ 添加客户（初始状态：TouchBase）")
    col1, col2 = st.columns(2)
    with col1:
        company = st.text_input("Company Name *")
        address = st.text_area("Address")
        contact = st.text_input("Contact")
        customer_email = st.text_input("客户邮箱")
        customer_need  = st.text_area("客户需求")
        business = st.text_input("业务")

        
    # ---- 表单右侧（替换原来的 sales + channel 那几行）----
    with col2:
        pref_whs = st.text_input("Preferred WHS Location")
        sales = st.text_input("销售 *")
    
        # ✅ 新增一个输入框
        sales_info = st.text_area("客户销售信息")
        # 从已有数据收集渠道
        existing_channels = []
        try:
            existing_channels = sorted([
                str(x).strip() for x in df.get("渠道", pd.Series(dtype=str)).dropna().unique()
                if str(x).strip() != ""
            ])
        except Exception:
            pass
    
        # 常用内置渠道
        builtin_channels = ["销售自拓", "客户referral"]
    
        # 合并去重
        channel_options = []
        for x in builtin_channels + existing_channels:
            if x not in channel_options:
                channel_options.append(x)
    
        # 选择已有渠道或自定义
        channel_choice = st.selectbox("渠道（可选或手动输入）*", options=["自定义填写"] + channel_options)
        channel_custom = ""
        if channel_choice == "自定义填写":
            channel_custom = st.text_input("请输入渠道名称 *", placeholder="例如：展会线索 / 官网咨询 / 合作伙伴推荐 …")
    
    if st.button("保存为新客户（记录 TouchBase 时间）", type="primary", use_container_width=True):
        channel_val = channel_custom.strip() if channel_choice == "自定义填写" else channel_choice.strip()
    
        if not company.strip():
            st.error("Company Name 必填")
        elif not validate_phone_or_email(contact):
            st.error("Contact 至少提供姓名/电话/邮箱之一")
        elif not sales.strip():
            st.error("销售 必填")
        elif not channel_val:
            st.error("渠道 必填")
        else:
            cid = gen_customer_id(); ts = now_str()
            ts_cols_init = {col: "" for col in STATUS_TS_COLS}
            ts_cols_init[f"{PIPELINE_STEPS[0]}_时间"] = ts
    
            row = {
                "customer_id": cid,
                "Company Name": company.strip(),
                "Address": address.strip(),
                "Contact": contact.strip(),
                "客户邮箱": customer_email.strip(),   # ✅ 新增
                "业务": business.strip(),
                "Preferred WHS Location": pref_whs.strip(),
                "渠道": channel_val,
                "客户需求": customer_need.strip(),   # ✅ 新增
                "客户销售信息": sales_info.strip(),      # ✅
                "当前状态": PIPELINE_STEPS[0],
                **ts_cols_init,
                "销售": sales.strip(),
            }
            write_new_row(ws, [row.get(c, "") for c in COLUMNS])
            st.success(f"✅ 新客户已创建：{company}（ID: {cid}），当前状态=TouchBase，时间={ts}")
            st.cache_data.clear()
            _goto("progress", cid)



# =================== 页面：⏩ 推进状态 & 添加备注 ===================
else:
    st.header("⏩ 推进状态 & 添加备注")

    # 返回列表按钮
    if st.button("⬅ 返回客户列表", use_container_width=True):
        _goto("view")

    cid = st.session_state.selected_customer_id
    if not cid:
        st.info("未选中客户：请先在『查看客户』点击某一行的『查看』。"); st.stop()

    if df.empty or cid not in df["customer_id"].values:
        st.error("选中的客户不存在或数据已变更。请返回列表重试。"); st.stop()

    idx = df.index[df["customer_id"] == cid][0]
    row = df.loc[idx].to_dict()
    cname = row["Company Name"]
    st.session_state.selected_customer_name = cname

    def _show_val(v):
        vv = str(v or "").strip()
        return vv if vv else "—"
    
    st.subheader("客户信息")
    
    colL, colR = st.columns(2)
    with colL:
        st.markdown(f"**Company Name**：{_show_val(row.get('Company Name',''))}")
        st.markdown(f"**Address**：{_show_val(row.get('Address',''))}")
        st.markdown(f"**Contact**：{_show_val(row.get('Contact',''))}")
        st.markdown(f"**业务**：{_show_val(row.get('业务',''))}")
        st.markdown(f"**客户邮箱**：{_show_val(row.get('客户邮箱',''))}")
        st.markdown(f"**客户需求**：{_show_val(row.get('客户需求',''))}")
    with colR:
        st.markdown(f"**Preferred WHS Location**：{_show_val(row.get('Preferred WHS Location',''))}")
        st.markdown(f"**渠道**：{_show_val(row.get('渠道',''))}")
        st.markdown(f"**销售**：{_show_val(row.get('销售',''))}")
        st.markdown(f"**ID**：`{cid}`")
        st.markdown(f"**销售信息**：{_show_val(row.get('销售信息',''))}")
    
    st.markdown(
        f"**当前状态**：{row['当前状态']}  \n"
        f"**是否完成**：{'是' if row['当前状态']=='Fulfill' else '否'}"
    )


    # 时间节点
    with st.expander("🕒 各状态时间节点（概览）", expanded=True):
        time_cols = {s: f"{s}_时间" for s in PIPELINE_STEPS}
        st.markdown("\n".join([f"- **{s}**：{row.get(col, '') or '—'}" for s, col in time_cols.items()]))

    st.divider()

    # ========== 沉睡 / 唤醒 按钮 ==========
    def _compute_latest_reached_stage(the_row: dict) -> str | None:
        """返回已到达的最近阶段（按时间戳最大值）；若均无时间戳返回 None。"""
        pairs = []
        for s in PIPELINE_STEPS:
            dt = _parse_dt_flex(the_row.get(f"{s}_时间", ""))
            if dt:
                pairs.append((s, dt))
        if not pairs:
            return None
        pairs.sort(key=lambda x: x[1])  # 最晚在最后
        return pairs[-1][0]

    col_sleep1, col_sleep2 = st.columns(2)
    with col_sleep1:
        if row["当前状态"] != SLEEPING_STATUS:
            if st.button("😴 设为沉睡", use_container_width=True):
                row["当前状态"] = SLEEPING_STATUS
                new_row = [str(row.get(c, "")) for c in COLUMNS]
                update_row(ws, idx, new_row, COLUMNS)
                st.success("已设为『沉睡』。该客户将不再触发未联系预警。")
                st.cache_data.clear(); _rerun()
    with col_sleep2:
        if row["当前状态"] == SLEEPING_STATUS:
            if st.button("🔔 唤醒（回到最近阶段/TouchBase）", type="primary", use_container_width=True):
                latest_stage = _compute_latest_reached_stage(row)
                if latest_stage is None:
                    # 从未有任何阶段时间戳：回到 TouchBase，并补时间
                    row["当前状态"] = "TouchBase"
                    row["TouchBase_时间"] = now_str()
                else:
                    row["当前状态"] = latest_stage
                    # 不改动历史时间戳
                new_row = [str(row.get(c, "")) for c in COLUMNS]
                update_row(ws, idx, new_row, COLUMNS)
                st.success(f"已唤醒，当前状态：{row['当前状态']}")
                st.cache_data.clear(); _rerun()

    # ========== 推进状态（仅前进一步；沉睡时不显示推进） ==========
    cur_status = row["当前状态"]
    nxt = allowed_next_steps(cur_status)
    if nxt:
        next_status = nxt[0]
        if st.button(f"⏩ 推进到：{next_status}（自动记录时间戳）", type="primary", use_container_width=True, key="progress_next"):
            row["当前状态"] = next_status; ts = now_str()
            row[f"{next_status}_时间"] = ts
            new_row = [str(row.get(c, "")) for c in COLUMNS]
            update_row(ws, idx, new_row, COLUMNS)
            st.success(f"已推进到【{next_status}】 ，时间={ts}")
            st.cache_data.clear()
            _rerun()
    else:
        if cur_status == SLEEPING_STATUS:
            st.info("当前处于【沉睡】状态：请先『唤醒』再进行推进。")
        else:
            st.info("当前已处于最终状态【Fulfill】（客户完成）。")

    st.divider()

    # 备注：仅当前及之前阶段可新增（看板式/表格化 + 批量保存）
    st.markdown("### 📝 各阶段备注（仅已到达及之前阶段可添加）")
    try:
        cur_idx = PIPELINE_STEPS.index(cur_status) if cur_status in PIPELINE_STEPS else -1
    except Exception:
        cur_idx = -1

    for status in PIPELINE_STEPS:
        status_idx = PIPELINE_STEPS.index(status)
        # 沉睡状态下：允许查看所有阶段备注，但只允许到达过的阶段（<= 最近阶段）新增
        can_add = (cur_status != SLEEPING_STATUS and status_idx <= max(cur_idx, 0)) or \
                  (cur_status == SLEEPING_STATUS and status_idx <= PIPELINE_STEPS.index(_compute_latest_reached_stage(row) or "TouchBase"))
        is_current = (status == cur_status)

        with st.expander(f"【{status}】备注", expanded=is_current):
            # 仅对允许添加的阶段才读/写，其他阶段不加载以省配额
            if not can_add:
                st.info("尚未推进到该阶段（为降低读取配额，此处暂不加载/创建备注页签）。")
                continue

            notes_ws = _get_or_create_notes_ws(gc, sh, status)
            notes_df = read_notes_df(notes_ws, ws_cache_key(notes_ws))
            my_notes = notes_df[notes_df["customer_id"] == cid].copy()

            # 直达链接
            try:
                gid = notes_ws._properties.get("sheetId")
                link = f"https://docs.google.com/spreadsheets/d/{sh.id}/edit#gid={gid}"
                st.caption(f"目标页签：**{notes_ws.title}** · [在 Google Sheets 中打开]({link})")
            except Exception:
                pass

            # == 看板式/表格化展示 + 批量更新 ==
            my_notes["__abs_idx__"] = my_notes.index  # 工作表中的绝对行（0-based）
            def _to_bool(x): return str(x).strip() == "是"

            if "完成" not in notes_df.columns:
                ensure_header(notes_ws, NOTE_COLUMNS)
                if "完成" not in my_notes.columns:
                    my_notes["完成"] = ""

            my_notes["完成_bool"] = my_notes["完成"].map(_to_bool)
            my_notes["_ts_sort_"] = pd.to_datetime(my_notes["创建时间"], errors="coerce")
            my_notes = my_notes.sort_values("_ts_sort_", ascending=False)

            # 顶部过滤区
            c1, c2 = st.columns([3,1])
            with c1:
                kw_note = st.text_input("🔍 关键字过滤（备注内容）", key=f"kw_{status}_{cid}")
            with c2:
                st.caption("（默认按创建时间倒序显示）")

            view = my_notes.copy()
            if kw_note and kw_note.strip():
                lk = kw_note.strip().lower()
                view = view[view["内容"].fillna("").str.lower().str.contains(lk)]

            todo_view = view[~view["完成_bool"]]
            done_view = view[view["完成_bool"]]

            tabs = st.tabs([
                f"🗂 待办（{len(todo_view)}）",
                f"✅ 已完成（{len(done_view)}）",
                f"📄 全部（{len(view)}）"
            ])

            def _render_editor(df_view: pd.DataFrame, tab_key: str):
                if df_view.empty:
                    st.caption("暂无记录。")
                    return None, None
            
                # 保留联结键
                df_show = df_view[["__abs_idx__", "内容", "创建时间", "完成_bool"]].copy()
                df_show = df_show.rename(columns={"完成_bool": "已完成"})
            
                edited = st.data_editor(
                    df_show,
                    key=f"ed_{status}_{cid}_{tab_key}",
                    use_container_width=True,
                    hide_index=True,               # 隐藏行索引
                    num_rows="fixed",
                    column_config={
                        "__abs_idx__": st.column_config.TextColumn("RID", help="内部行号（只读）"),
                        "内容": st.column_config.TextColumn(disabled=True),
                        "创建时间": st.column_config.TextColumn(disabled=True),
                        "已完成": st.column_config.CheckboxColumn("已完成"),
                    },
                    disabled=["__abs_idx__", "内容", "创建时间"],   # 联结键设为只读
                    height=min(420, 48 + 38 * len(df_show))
                )
                do_save = st.button("💾 保存更改", key=f"save_{status}_{cid}_{tab_key}", use_container_width=True)
                return edited, do_save


            with tabs[0]:
                edited_todo, save_todo = _render_editor(todo_view, "todo")
            with tabs[1]:
                edited_done, save_done = _render_editor(done_view, "done")
            with tabs[2]:
                edited_all, save_all  = _render_editor(view, "all")

            def _apply_changes(original_view: pd.DataFrame, edited_df: pd.DataFrame):
                if edited_df is None or original_view is None or original_view.empty:
                    return
                merged = original_view.merge(
                    edited_df.rename(columns={"已完成": "已完成_new"}),
                    left_on="__abs_idx__", right_on="__abs_idx__", how="left", suffixes=("", "_new")
                )
                changed = merged[
                    (merged["已完成_new"].notna()) &
                    (merged["完成_bool"].astype(bool) != merged["已完成_new"].astype(bool))
                ]
                if changed.empty:
                    st.info("没有检测到更改。")
                    return

                header_cols = list(notes_df.columns) if list(notes_df.columns) else NOTE_COLUMNS
                ok_cnt, fail_cnt = 0, 0
                for _, rr in changed.iterrows():
                    row_zero_based = int(rr["__abs_idx__"])
                    new_done = bool(rr["已完成_new"])
                    try:
                        set_note_done(notes_ws, header_cols, row_zero_based, new_done)
                        ok_cnt += 1
                    except Exception as e:
                        fail_cnt += 1
                        st.warning(f"行 {row_zero_based+2} 更新失败：{e}")

                if ok_cnt:
                    st.success(f"已更新完成状态：{ok_cnt} 条")
                    st.cache_data.clear()
                    _rerun()
                elif fail_cnt:
                    st.error("全部更新失败，请稍后重试。")

            if save_todo: _apply_changes(todo_view, edited_todo)
            if save_done: _apply_changes(done_view, edited_done)
            if save_all:  _apply_changes(view, edited_all)

            # 快速添加（默认未完成）
            with st.form(f"quick_add_{status}_{cid}", clear_on_submit=True):
                qtxt = st.text_input("✍️ 快速添加一条待办", placeholder=f"{status} 阶段需要跟进的事项…")
                if st.form_submit_button("➕ 添加", use_container_width=True):
                    if not qtxt.strip():
                        st.error("内容不能为空")
                    else:
                        _safe_append_row(notes_ws, [uuid.uuid4().hex[:8].upper(), cid, qtxt.strip(), now_str(), ""])
                        st.toast("已添加一条待办", icon="📝")
                        st.cache_data.clear()
                        _rerun()
