# -*- coding: utf-8 -*-
import streamlit as st
import requests
import time
import threading
import io
import re
from base64 import b64encode
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque

# ─── Logging setup ───────────────────────────────────────────────────────────
import logging
import traceback
from datetime import datetime

_log_buffer = []
_log_lock = threading.Lock() if 'threading' in dir() else None

def log_event(level, msg, exc=None):
    """Thread-safe logging to in-memory buffer."""
    import threading as _threading
    ts = datetime.now().strftime("%H:%M:%S")
    entry = f"[{ts}] {level}: {msg}"
    if exc:
        entry += f"\n  ERROR: {exc}\n  {traceback.format_exc().strip()}"
    with _threading.Lock():
        _log_buffer.append(entry)
        # Keep last 500 entries
        if len(_log_buffer) > 500:
            _log_buffer.pop(0)

def get_log_text():
    return "\n".join(_log_buffer)

def send_error_email(gmail_user, gmail_pass, email_to, subject, body):
    """Send error/crash notification email."""
    try:
        import smtplib
        from email.mime.text import MIMEText
        msg = MIMEText(body)
        msg["From"] = gmail_user
        msg["To"] = email_to
        msg["Subject"] = subject
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(gmail_user, gmail_pass)
            server.sendmail(gmail_user, email_to, msg.as_string())
    except:
        pass

# ─── Page config ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Companies House Prospector",
    page_icon="\U0001f3e2",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─── Styling ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .stApp { background: #f7f6f2; }
    .main-header {
        background: #1a4a2e;
        color: white;
        padding: 1rem 1.5rem;
        border-radius: 8px;
        margin-bottom: 1rem;
        font-family: Georgia, serif;
        font-size: 1.4rem;
        font-weight: bold;
    }
    .metric-box {
        background: white;
        border: 1px solid #e0e8e0;
        border-radius: 6px;
        padding: 0.6rem 1rem;
        text-align: center;
    }
    .metric-label { font-size: 0.75rem; color: #6b6960; font-family: Georgia, serif; }
    .metric-value { font-size: 1.1rem; color: #1a4a2e; font-weight: bold; font-family: Courier, monospace; }
    div[data-testid="stSidebar"] { background: white; }
    .stButton > button {
        background: #1a4a2e;
        color: white;
        border: none;
        border-radius: 6px;
        font-family: Georgia, serif;
    }
    .stButton > button:hover { background: #2d6b47; }
</style>
""", unsafe_allow_html=True)

# ─── Constants ────────────────────────────────────────────────────────────────
API_BASE = "https://api.company-information.service.gov.uk"

SIC_OPTIONS = [
    ("Accounting / bookkeeping (69201)", "69201"),
    ("Advertising agencies (73110)", "73110"),
    ("Antiques (47791)", "47791"),
    ("Architectural activities (71111)", "71111"),
    ("Architecture (71112)", "71112"),
    ("Art galleries retail (47781)", "47781"),
    ("Auditing (69202)", "69202"),
    ("Barristers (69101)", "69101"),
    ("Building construction (41202)", "41202"),
    ("Car dealerships (45111)", "45111"),
    ("Care agencies / staffing (78300)", "78300"),
    ("Catering / event catering (56210)", "56210"),
    ("Cosmetic / plastic surgery clinics (86102)", "86102"),
    ("Craft brewing (11050)", "11050"),
    ("Dental practices (86220)", "86220"),
    ("Distilling / spirits (11010)", "11010"),
    ("Engineering consultancy (71121)", "71121"),
    ("Estate agents (68310)", "68310"),
    ("Event management - conferences (82302)", "82302"),
    ("Event management - exhibitions (82301)", "82301"),
    ("General building contractors (41201)", "41201"),
    ("GP practices (86210)", "86210"),
    ("HR consulting (70229)", "70229"),
    ("Immigration consultants (69109)", "69109"),
    ("Insurance brokers (66220)", "66220"),
    ("IT consultancy (62020)", "62020"),
    ("Management consulting (70229)", "70229"),
    ("Market research (73200)", "73200"),
    ("Mortgage brokers (66190)", "66190"),
    ("Opticians (86230)", "86230"),
    ("Physical wellbeing / spa (96040)", "96040"),
    ("Physiotherapy (86901)", "86901"),
    ("PR / communications (70210)", "70210"),
    ("Property buying & selling (68100)", "68100"),
    ("Property development (41100)", "41100"),
    ("Property management (68320)", "68320"),
    ("Quantity surveying (74902)", "74902"),
    ("Racehorse owners (93191)", "93191"),
    ("Recruitment (78200)", "78200"),
    ("Software development (62012)", "62012"),
    ("Solicitors (69102)", "69102"),
    ("Sound recording / music publishing (59200)", "59200"),
    ("Surveying (71112)", "71112"),
    ("Tax consulting (69203)", "69203"),
    ("Tour operators (79120)", "79120"),
    ("Travel agencies (79110)", "79110"),
    ("TV programme production (59113)", "59113"),
    ("Veterinary (75000)", "75000"),
    ("Video production (59112)", "59112"),
    ("Vocational training (85320)", "85320"),
]

# ─── Core functions (unchanged from desktop version) ─────────────────────────

class RateLimiter:
    def __init__(self, max_calls=575, window=300):
        self.max_calls = max_calls
        self.window = window
        self.calls = deque()
        self.lock = threading.Lock()
        self.paused = False

    def record_call(self):
        with self.lock:
            self.calls.append(time.time())

    def calls_in_window(self):
        with self.lock:
            now = time.time()
            cutoff = now - self.window
            while self.calls and self.calls[0] < cutoff:
                self.calls.popleft()
            return len(self.calls)

    def wait_if_needed(self):
        while True:
            count = self.calls_in_window()
            if count < self.max_calls:
                self.paused = False
                self.record_call()
                return
            self.paused = True
            with self.lock:
                wait_time = (self.calls[0] + self.window) - time.time() + 0.1 if self.calls else 1
            time.sleep(max(0.1, wait_time))

_rate_limiter = RateLimiter()

def ch_get(path, api_key):
    _rate_limiter.wait_if_needed()
    auth = "Basic " + b64encode(f"{api_key}:".encode()).decode()
    r = requests.get(API_BASE + path, headers={"Authorization": auth}, timeout=20)
    if r.status_code == 429:
        time.sleep(5)
        _rate_limiter.wait_if_needed()
        r = requests.get(API_BASE + path, headers={"Authorization": auth}, timeout=20)
    r.raise_for_status()
    return r.json()

def fmt_currency(val):
    if val is None: return ""
    try:
        v = float(val)
        if abs(v) >= 1_000_000:
            return f"\u00a3{v/1_000_000:.1f}m"
        elif abs(v) >= 1_000:
            return f"\u00a3{v/1_000:.0f}k"
        else:
            return f"\u00a3{v:.0f}"
    except:
        return ""

def title_case_company(name):
    if not name: return name
    PRESERVE = {"UK","LLP","LTD","PLC","USA","IT","HR","PR"}
    words = name.split()
    result = []
    for i, w in enumerate(words):
        clean = w.strip(".,()&")
        if clean.upper() in PRESERVE and i > 0:
            result.append(clean.upper())
        else:
            result.append("-".join(p.capitalize() for p in w.split("-")))
    return " ".join(result)

def split_director_name(full_name):
    if not full_name: return "", ""
    TITLES = {"mr","mrs","ms","miss","dr","prof","sir","dame","rev","rt","hon",
              "lord","lady","cllr","capt","maj","col","lt","cmdr","qc","kc"}
    parts = full_name.strip().split()
    while parts and parts[0].lower().rstrip(".") in TITLES:
        parts = parts[1:]
    if not parts: return "", ""
    def tc(s): return "-".join(w.capitalize() for w in s.split("-"))
    caps = [p for p in parts if p.replace("-","").isupper() and len(p) > 1]
    lower = [p for p in parts if not p.replace("-","").isupper()]
    if caps:
        surname = tc(caps[-1])
        first = tc(lower[0]) if lower else tc(parts[0])
    else:
        first = tc(parts[0])
        surname = tc(parts[-1]) if len(parts) > 1 else ""
    return first, surname

def fetch_financials(company_number, api_key):
    result = {"accounts_date":"","turnover":"","total_assets":"","net_assets":"",
              "fixed_assets":"","current_assets":"","employees":"","accountant":""}
    try:
        from bs4 import BeautifulSoup
        auth = "Basic " + b64encode(f"{api_key}:".encode()).decode()
        headers = {"Authorization": auth}
        _rate_limiter.wait_if_needed()
        fh = requests.get(f"{API_BASE}/company/{company_number}/filing-history",
                         params={"category":"accounts","items_per_page":10},
                         headers=headers, timeout=12)
        if fh.status_code == 429:
            time.sleep(3); _rate_limiter.wait_if_needed()
            fh = requests.get(f"{API_BASE}/company/{company_number}/filing-history",
                             params={"category":"accounts","items_per_page":10},
                             headers=headers, timeout=12)
        if fh.status_code != 200: return result
        filings = [f for f in fh.json().get("items",[])
                   if "dormant" not in f.get("description","").lower()]
        if not filings: return result
        latest = filings[0]
        result["accounts_date"] = latest.get("action_date", latest.get("date",""))
        doc_meta_url = latest.get("links",{}).get("document_metadata","")
        if not doc_meta_url: return result
        _rate_limiter.wait_if_needed()
        dm = requests.get(doc_meta_url, headers=headers, timeout=12)
        if dm.status_code != 200: return result
        meta = dm.json()
        doc_url = meta.get("links",{}).get("document","")
        if not doc_url: return result
        resources = meta.get("resources",{})
        if "application/xhtml+xml" not in resources: return result
        doc_r = requests.get(doc_url, headers={**headers,"Accept":"application/xhtml+xml"}, timeout=25)
        if doc_r.status_code != 200: return result
        soup = BeautifulSoup(doc_r.content, "html.parser")

        def get_val(soup, tag_names):
            for tag_name in tag_names:
                for tag in soup.find_all(attrs={"name":True}):
                    name_attr = tag.get("name","")
                    bare = name_attr.split(":")[-1] if ":" in name_attr else name_attr
                    if bare.lower() != tag_name.lower(): continue
                    ctx = tag.get("contextref","")
                    if any(x in ctx.lower() for x in ["prior","previous","preceding"]): continue
                    sign = tag.get("sign","")
                    scale = int(tag.get("scale","0") or "0")
                    try:
                        raw = tag.get_text(strip=True).replace(",","").replace(" ","").replace("\xa0","")
                        if not raw or raw in ("-","—"): continue
                        val = float(raw) * (10**scale)
                        if sign == "-": val = -val
                        if val != 0: return val
                    except: continue
            return None

        def fv(v): return fmt_currency(v) if v is not None else ""
        def ev(v):
            if v is None: return ""
            try:
                i = int(float(v))
                if i <= 0 or i > 5000 or (1980 <= i <= 2040): return ""
                return str(i)
            except: return ""

        result["total_assets"]   = fv(get_val(soup,["TotalAssetsLessCurrentLiabilities","TotalAssets","BalanceSheetTotal","Assets"]))
        result["net_assets"]     = fv(get_val(soup,["NetAssetsLiabilities","NetAssets","ShareholdersEquity","Equity"]))
        result["fixed_assets"]   = fv(get_val(soup,["FixedAssets","TotalFixedAssets","NonCurrentAssets"]))
        result["current_assets"] = fv(get_val(soup,["CurrentAssets","TotalCurrentAssets"]))
        result["employees"]      = ev(get_val(soup,["AverageNumberEmployeesDuringPeriod","NumberEmployees","AverageNumberPersonsEmployed"]))

        if not result["employees"]:
            text = soup.get_text().lower()
            MONTHS = ["january","february","march","april","may","june","july",
                      "august","september","october","november","december"]
            for pat in [r"average\s+number\s+of\s+(?:employees|persons\s+employed)\s+(?:during\s+the\s+(?:year|period)\s+)?(?:was|were|:)\s*(\d{1,4})",
                        r"number\s+of\s+employees[^.]{0,40}(?:was|were|:)\s*(\d{1,4})"]:
                m = re.search(pat, text)
                if m:
                    v = int(m.group(1))
                    surrounding = text[max(0,m.start(1)-30):m.start(1)+10]
                    if 0 < v < 1000 and not (1980 <= v <= 2040) and not any(mo in surrounding for mo in MONTHS):
                        result["employees"] = str(v)
                        break

        # Extract accountant/auditor name from iXBRL text
        try:
            import re as _re
            full_text = soup.get_text(separator=" ", strip=True)
            accountant = ""

            # Known firm suffixes to anchor extraction
            SUFFIXES = r"(?:LLP|Chartered Accountants|Certified Accountants|Chartered Certified Accountants|& Co(?:\.|mpany)?|Accountants)"

            # Priority: look for firm name after explicit trigger phrases
            trigger_pat = (
                r"(?:prepared by|statutory auditors?|reporting accountants?|"
                r"independent auditors?|audited by|accounts? (?:have been )?prepared by)"
                r"[:\s]+([A-Z][A-Za-z0-9 &,\.\-]{2,50}?" + SUFFIXES + r")"
            )
            m = _re.search(trigger_pat, full_text)
            if m:
                accountant = m.group(1).strip().rstrip(".,")
            else:
                # Fallback: find any firm with a known suffix
                fallback_pat = r"([A-Z][A-Za-z0-9 &,\.\-]{2,50}?" + SUFFIXES + r")"
                for m in _re.finditer(fallback_pat, full_text):
                    candidate = m.group(1).strip().rstrip(".,")
                    # Skip obvious non-accountant matches
                    skip = ["the company", "the directors", "companies house", "hmrc",
                            "limited company", "association of", "institute of",
                            "liability partnership", "recruitment", "staffing",
                            "employment", "personnel", "limited liability"]
                    if any(s in candidate.lower() for s in skip):
                        continue
                    if len(candidate) > 4:
                        accountant = candidate
                        break

            # Clean up: truncate at 60 chars, strip trailing junk
            if accountant:
                # Remove anything after a pipe
                accountant = accountant.split("|")[0].strip()
                # Strip leading boilerplate words
                for prefix in ["Pages For Filing With Registrar ", "PAGES FOR FILING WITH REGISTRAR "]:
                    if accountant.startswith(prefix):
                        accountant = accountant[len(prefix):]
                accountant = accountant.strip()[:60]

            result["accountant"] = accountant
        except:
            pass
    except: pass
    return result

def calc_score(fin):
    score = 0
    def parse_val(s):
        if not s: return None
        try:
            s = s.replace("\u00a3","").replace(",","").strip()
            neg = s.startswith("-"); s = s.lstrip("-")
            mult = 1_000_000 if s.endswith("m") else (1_000 if s.endswith("k") else 1)
            return float(s.rstrip("mk")) * mult * (-1 if neg else 1)
        except: return None
    na = parse_val(fin.get("net_assets",""))
    if na:
        if na > 500_000: score += 2
        elif na > 100_000: score += 1
    emp = fin.get("employees","")
    if emp:
        try:
            e = int(emp)
            if e >= 20: score += 2
            elif e >= 5: score += 1
        except: pass
    ca = parse_val(fin.get("current_assets",""))
    if ca and ca > 200_000: score += 1
    return score

def fetch_all_for_sic(sic_code, base_params, api_key):
    auth = "Basic " + b64encode(f"{api_key}:".encode()).decode()
    headers = {"Authorization": auth}
    params_base = {**base_params}
    if sic_code: params_base["sic_codes"] = sic_code
    items = []
    start = 0
    total = None
    while True:
        params = {**params_base, "size": 100, "start_index": start}
        _rate_limiter.wait_if_needed()
        r = requests.get(API_BASE + "/advanced-search/companies",
                        params=params, headers=headers, timeout=15)
        if r.status_code not in (200,): break
        data = r.json()
        batch = data.get("items", data.get("companies",[]))
        if total is None: total = data.get("hits", data.get("total_results",0))
        if not batch: break
        items.extend(batch)
        start += len(batch)
        if start >= (total or 0) or start >= 5000: break
    return items

# ─── Global job tracker ──────────────────────────────────────────────────────
# Persists across Streamlit sessions on the same Railway container
_job_status = {
    "running": False,
    "started_at": None,
    "search_params": "",
    "dir_done": 0,
    "fin_done": 0,
    "total": 0,
    "completed": False,
    "email_sent": False,
    "error": None,
}
_job_lock = threading.Lock()

def update_job(**kwargs):
    with _job_lock:
        _job_status.update(kwargs)

def get_job():
    with _job_lock:
        return dict(_job_status)

# ─── Email helper ────────────────────────────────────────────────────────────

def send_email_results(gmail_user, gmail_pass, email_to, excel_buf, csv_data, search_date, criteria):
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.base import MIMEBase
    from email.mime.text import MIMEText
    from email import encoders
    try:
        msg = MIMEMultipart()
        msg["From"] = gmail_user
        msg["To"] = email_to
        msg["Subject"] = f"Companies House Prospector Results — {search_date}"

        # Body
        body_lines = ["Your Companies House Prospector search has completed.", ""]
        for k, v in criteria.items():
            body_lines.append(f"{k}: {v}")
        body_lines.append("Please find the Excel and CSV results attached.")
        msg.attach(MIMEText("\n".join(body_lines), "plain"))

        # Attach Excel
        part_xl = MIMEBase("application", "octet-stream")
        part_xl.set_payload(excel_buf)
        encoders.encode_base64(part_xl)
        part_xl.add_header("Content-Disposition", f'attachment; filename="prospector_results_{search_date}.xlsx"')
        msg.attach(part_xl)

        # Attach CSV
        part_csv = MIMEBase("application", "octet-stream")
        part_csv.set_payload(csv_data.encode("utf-8-sig"))
        encoders.encode_base64(part_csv)
        part_csv.add_header("Content-Disposition", f'attachment; filename="prospector_results_{search_date}.csv"')
        msg.attach(part_csv)

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(gmail_user, gmail_pass)
            server.sendmail(gmail_user, email_to, msg.as_string())
        return True
    except Exception as e:
        return str(e)

# ─── Streamlit UI ─────────────────────────────────────────────────────────────

st.markdown('<div class="main-header">\U0001f3e2 Companies House Prospector</div>', unsafe_allow_html=True)

# ─── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### \U0001f511 API Key")
    # Load from Streamlit secrets if available
    import os
    try:
        api_key = st.secrets.get("CH_API_KEY") or os.environ.get("CH_API_KEY","")
        if api_key:
            st.success("API key loaded ✅")
        else:
            api_key = st.text_input("Companies House API Key", type="password",
                                     value=st.session_state.get("api_key",""),
                                     placeholder="Paste your API key here")
            if api_key: st.session_state["api_key"] = api_key
    except:
        api_key = os.environ.get("CH_API_KEY","")
        if api_key:
            st.success("API key loaded ✅")
        else:
            api_key = st.text_input("Companies House API Key", type="password",
                                     value=st.session_state.get("api_key",""),
                                     placeholder="Paste your API key here")
            if api_key: st.session_state["api_key"] = api_key

    # Email settings
    st.markdown("---")
    st.markdown("### 📧 Email Results")
    try:
        gmail_user = st.secrets.get("GMAIL_USER") or os.environ.get("GMAIL_USER","")
        gmail_pass = st.secrets.get("GMAIL_APP_PASSWORD") or os.environ.get("GMAIL_APP_PASSWORD","")
        email_configured = bool(gmail_user and gmail_pass)
    except:
        gmail_user = os.environ.get("GMAIL_USER","")
        gmail_pass = os.environ.get("GMAIL_APP_PASSWORD","")
        email_configured = bool(gmail_user and gmail_pass)
    send_email = st.checkbox("Email results when complete", value=email_configured)
    email_to = st.text_input("Send to", value="david.sillars@quilterfa.com", placeholder="your@email.com")

    st.markdown("---")
    st.markdown("### \U0001f4cd Location")
    location = st.text_input("Location", value="Surrey")

    st.markdown("### \U0001f3ed Industry")
    st.caption("Hold Ctrl/Cmd to select multiple")
    sic_labels = [s[0] for s in SIC_OPTIONS]
    sic_codes  = [s[1] for s in SIC_OPTIONS]
    selected_sic_labels = st.multiselect("Industry / SIC code", sic_labels, default=[])

    st.markdown("### \U0001f3e2 Company Type")
    type_ltd = st.checkbox("Private Limited (Ltd)", value=True)
    type_llp = st.checkbox("LLP", value=True)
    type_plc = st.checkbox("Public (PLC)", value=False)

    st.markdown("### \u2605 Quality Filters")
    excl_dormant = st.checkbox("Exclude dormant", value=True)
    min_age = st.number_input("Min age (years)", value=3, min_value=0, max_value=50)
    max_age = st.number_input("Max age (years)", value=0, min_value=0, max_value=100,
                               help="0 = no limit")
    min_net_assets = st.number_input("Min net assets (\u00a3)", value=0, min_value=0)

    st.markdown("### \U0001f4c8 Financial Data")
    fetch_financials_flag = st.checkbox("Fetch financials (slower)", value=True)
    st.caption("~3s per company. Uncheck for fast search.")

    st.markdown("### \U0001f465 Employee Filter")
    col3, col4 = st.columns(2)
    with col3: emp_min = st.number_input("Min", value=0, min_value=0, key="emp_min")
    with col4: emp_max = st.number_input("Max", value=0, min_value=0, key="emp_max",
                                          help="0 = no limit")

    st.markdown("### \u2699\ufe0f Output")
    one_per_company = st.checkbox("One contact per company", value=True)

    st.markdown("---")
    search_btn = st.button("\U0001f50d Search Companies House", use_container_width=True)
    if st.button("🗑 Clear Results", use_container_width=True):
        st.session_state.results = []
        st.session_state.director_cache = {}
        st.session_state.financials_cache = {}
        st.rerun()

# ─── Main area ────────────────────────────────────────────────────────────────

if "results" not in st.session_state:
    st.session_state.results = []
    st.session_state.director_cache = {}
    st.session_state.financials_cache = {}

if search_btn:
    if not api_key:
        st.error("Please enter your API key in the sidebar.")
    elif not selected_sic_labels:
        st.warning("Please select at least one industry.")
    else:
        selected_sics = [sic_codes[sic_labels.index(l)] for l in selected_sic_labels]

        # Build base params
        selected_types = []
        if type_ltd: selected_types.append("ltd")
        if type_llp: selected_types.append("llp")
        if type_plc: selected_types.append("plc")

        base_params = {"location": location, "company_status": "active"}
        if selected_types: base_params["company_type"] = ",".join(selected_types)

        # Progress display
        progress_bar = st.progress(0)
        status_text = st.empty()

        # ── Stage 1: Fetch companies ──────────────────────────────────────────
        status_text.write("**Stage 1/4** — Fetching companies from Companies House...")
        all_items = []
        seen_numbers = set()

        for i, sic_code in enumerate(selected_sics):
            status_text.write(f"**Stage 1/4** — Searching {i+1}/{len(selected_sics)}: {selected_sic_labels[i]}...")
            fetched = fetch_all_for_sic(sic_code, base_params, api_key)
            for c in fetched:
                num = c.get("company_number","")
                if num and num not in seen_numbers:
                    seen_numbers.add(num)
                    all_items.append(c)
            progress_bar.progress(int((i+1)/len(selected_sics)*25))

        status_text.write(f"**Stage 1/4** — Found {len(all_items):,} companies. Filtering...")

        # Filter by age
        today = date.today()
        filtered = []
        for c in all_items:
            inc = c.get("date_of_creation","")
            if excl_dormant and "dormant" in c.get("company_status","").lower():
                continue
            if inc:
                try:
                    y,m2,d2 = inc.split("-")
                    age_yrs = (today - date(int(y),int(m2),int(d2))).days // 365
                    if min_age > 0 and age_yrs < min_age: continue
                    if max_age > 0 and age_yrs > max_age: continue
                except: pass
            filtered.append(c)
        all_items = filtered
        progress_bar.progress(30)

        # ── Stage 2 & 3: Directors and Financials in parallel ────────────────
        status_text.write(f"**Stages 2 & 3** — Loading directors and financials for {len(all_items):,} companies...")

        director_cache = {}
        financials_cache = {}
        dir_lock = threading.Lock()
        fin_lock = threading.Lock()
        dir_done = [0]
        fin_done = [0]
        total = len(all_items)

        def fetch_dir(c):
            num = c.get("company_number","")
            if not num: return num, []
            try:
                d = ch_get(f"/company/{num}/officers?items_per_page=10", api_key)
                active = [o for o in d.get("items",[])
                          if not o.get("resigned_on") and
                          o.get("officer_role","") in ("director","llp-designated-member","member")]
                return num, active
            except Exception as e:
                log_event("WARN", f"fetch_dir failed for {num}: {e}")
                return num, []

        def fetch_fin(c):
            num = c.get("company_number","")
            if not num: return num, {}
            try:
                return num, fetch_financials(num, api_key)
            except Exception as e:
                log_event("WARN", f"fetch_fin failed for {num}: {e}")
                return num, {}

        def run_dirs():
            try:
                with ThreadPoolExecutor(max_workers=6) as ex:
                    for future in as_completed({ex.submit(fetch_dir,c):c for c in all_items}):
                        try:
                            num, active = future.result()
                            with dir_lock: director_cache[num] = active
                        except Exception as e:
                            log_event("WARN", f"run_dirs future error: {e}")
                        with dir_lock: dir_done[0] += 1
            except Exception as e:
                log_event("ERROR", f"run_dirs thread crashed: {e}", e)

        def run_fins():
            if not fetch_financials_flag: return
            try:
                with ThreadPoolExecutor(max_workers=3) as ex:
                    for future in as_completed({ex.submit(fetch_fin,c):c for c in all_items}):
                        try:
                            num, fin = future.result()
                            with fin_lock: financials_cache[num] = fin
                        except Exception as e:
                            log_event("WARN", f"run_fins future error: {e}")
                        with fin_lock: fin_done[0] += 1
            except Exception as e:
                log_event("ERROR", f"run_fins thread crashed: {e}", e)

        update_job(
            running=True, completed=False, email_sent=False, error=None,
            started_at=time.time(), total=total,
            dir_done=0, fin_done=0,
            search_params=f"{location} | {', '.join(selected_sic_labels[:3])}"
                         + (f" +{len(selected_sic_labels)-3} more" if len(selected_sic_labels)>3 else "")
        )

        t1 = threading.Thread(target=run_dirs, daemon=False)
        t2 = threading.Thread(target=run_fins, daemon=False)
        t1.start(); t2.start()

        # Show live progress while threads run
        prog_placeholder = st.empty()
        search_start = time.time()
        last_done = [0]
        last_time = [time.time()]
        eta_seconds = [None]
        eta_min = [None]
        eta_max = [None]
        heartbeat_sent = [False]
        # Rolling window: list of (timestamp, done_work) snapshots for recent rate
        rate_history = []
        # Track actual API calls used per financial completed for accuracy
        actual_calls_per_fin = []

        while t1.is_alive() or t2.is_alive():
            d = dir_done[0]; f = fin_done[0]
            update_job(dir_done=d, fin_done=f)
            now = time.time()
            elapsed = now - search_start

            # Total units of work (dirs + fins weighted)
            fins_weight = 2 if fetch_financials_flag else 0
            total_work = total * (1 + fins_weight)
            done_work = d + (f * fins_weight)
            remaining_work = total_work - done_work

            # Track actual calls per financial to improve accuracy
            calls_used = _rate_limiter.calls_in_window()
            if f > 0:
                actual_calls_per_fin.append(calls_used / max(f, 1))

            # Rolling rate: snapshot every second, keep last 30s
            rate_history.append((now, done_work))
            rate_history = [(t, w) for t, w in rate_history if now - t <= 30]

            if done_work > 0 and elapsed > 3:
                # Recent rate (last 30s) - more responsive to rate limiting
                if len(rate_history) >= 2:
                    oldest_t, oldest_w = rate_history[0]
                    recent_rate = max((done_work - oldest_w) / max(now - oldest_t, 1), 0.001)
                else:
                    recent_rate = done_work / elapsed

                # Overall average rate since start
                avg_rate = done_work / elapsed

                # Blend: 60% recent, 40% average for stability
                blended_rate = (recent_rate * 0.6) + (avg_rate * 0.4)

                # Actual calls per financial (use measured value if available)
                if actual_calls_per_fin and f > 5:
                    measured_cpf = sum(actual_calls_per_fin[-5:]) / len(actual_calls_per_fin[-5:])
                    calls_per_fin = min(max(measured_cpf, 1.5), 6.0)  # cap between 1.5-6
                else:
                    calls_per_fin = 3.5

                # Remaining API calls needed
                fins_remaining = total - f
                dirs_remaining = total - d
                calls_needed = (dirs_remaining * 1.0) + (fins_remaining * calls_per_fin)
                calls_available = max(575 - calls_used, 0)

                # Base time estimate
                if blended_rate > 0:
                    base_eta = remaining_work / blended_rate
                else:
                    base_eta = 9999

                # Add rate limit pause time if we'll exceed window
                if calls_needed > calls_available:
                    extra_calls = calls_needed - calls_available
                    pause_cycles = extra_calls / 575
                    pause_time = pause_cycles * 300
                    base_eta += pause_time

                # If currently paused, add remaining wait time
                if _rate_limiter.paused:
                    base_eta += max(60, 300 - (elapsed % 300))

                # ETA range: ±20% for display
                eta_seconds[0] = base_eta
                eta_min[0] = base_eta * 0.8
                eta_max[0] = base_eta * 1.25

                last_done[0] = done_work
                last_time[0] = now

            # Heartbeat email at 5 minutes
            if send_email and email_to and email_configured and not heartbeat_sent[0] and elapsed >= 300:
                try:
                    import smtplib
                    from email.mime.text import MIMEText as _MIMEText
                    _msg = _MIMEText(
                        f"Your Companies House Prospector search is still running.\n\n"
                        f"Elapsed: {int(elapsed)//60}m {int(elapsed)%60}s\n"
                        f"Directors: {d:,}/{total:,}\n"
                        f"Financials: {f:,}/{total:,}\n\n"
                        f"You will receive another email when the search completes with your results attached."
                    )
                    _msg["From"] = gmail_user
                    _msg["To"] = email_to
                    _msg["Subject"] = f"Companies House Prospector — Search in progress"
                    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as _srv:
                        _srv.login(gmail_user, gmail_pass)
                        _srv.sendmail(gmail_user, email_to, _msg.as_string())
                    heartbeat_sent[0] = True
                except:
                    pass

            # Format ETA with range
            def fmt_eta(s):
                s = int(s)
                if s >= 3600: return f"{s//3600}h {(s%3600)//60}m"
                elif s >= 60: return f"{s//60}m {s%60}s"
                else: return f"{s}s"

            if eta_seconds[0] is not None and done_work > 0 and elapsed > 3:
                lo = fmt_eta(eta_min[0])
                hi = fmt_eta(eta_max[0])
                mid = fmt_eta(eta_seconds[0])
                if lo == hi:
                    eta_display = f"&nbsp;&nbsp; ⏱ **Est. remaining: {mid}**"
                else:
                    eta_display = f"&nbsp;&nbsp; ⏱ **Est. remaining: {lo} – {hi}**"
            elif done_work == 0 or elapsed <= 3:
                eta_display = "&nbsp;&nbsp; ⏱ *Calculating...*"
            else:
                eta_display = ""

            # Format elapsed
            el = int(elapsed)
            if el >= 60:
                elapsed_str = f"{el//60}m {el%60}s"
            else:
                elapsed_str = f"{el}s"

            prog_placeholder.markdown(
                f"**Directors:** {d:,}/{total:,} &nbsp;&nbsp; "
                f"**Financials:** {f:,}/{total:,} &nbsp;&nbsp; "
                f"**API calls:** {_rate_limiter.calls_in_window()}/575 &nbsp;&nbsp; "
                f"**Elapsed:** {elapsed_str}"
                + eta_display
                + (" &nbsp;&nbsp; ⏸ *Rate limit - waiting...*" if _rate_limiter.paused else "")
            )
            true_pct = int((done_work / max(total_work, 1)) * 100)
            progress_bar.progress(min(true_pct, 99), text=f"{true_pct}% complete")
            time.sleep(1)

        t1.join(); t2.join()
        prog_placeholder.empty()
        log_event("INFO", f"Threads complete. Directors: {dir_done[0]}/{total}, Financials: {fin_done[0]}/{total}")

        # Send crash email if threads died early
        if send_email and email_to and email_configured:
            if dir_done[0] < total * 0.9 or (fetch_financials_flag and fin_done[0] < total * 0.9):
                log_event("WARN", f"Search may have stopped early. Dir: {dir_done[0]}/{total}, Fin: {fin_done[0]}/{total}")
                send_error_email(
                    gmail_user, gmail_pass, email_to,
                    "Companies House Prospector — Search stopped early",
                    f"The search appears to have stopped before completing.\n\n"
                    f"Directors completed: {dir_done[0]}/{total}\n"
                    f"Financials completed: {fin_done[0]}/{total}\n"
                    f"Elapsed: {int(time.time()-search_start)//60}m\n\n"
                    f"Recent log:\n{get_log_text()[-3000:]}"
                )

        # ── Stage 4: Filter, score, sort ──────────────────────────────────────
        status_text.write("**Stage 4/4** — Filtering and scoring results...")
        progress_bar.progress(90)

        results = []
        for c in all_items:
            num = c.get("company_number","")
            fin = financials_cache.get(num,{})

            # Exclude companies flagged as dormant in company status
            if excl_dormant and "dormant" in c.get("company_status","").lower():
                continue

            # Net assets filter
            if min_net_assets > 0 and fin.get("net_assets",""):
                try:
                    s = fin["net_assets"].replace("\u00a3","").replace(",","").strip()
                    neg = s.startswith("-"); s = s.lstrip("-")
                    mult = 1_000_000 if s.endswith("m") else (1_000 if s.endswith("k") else 1)
                    val = float(s.rstrip("mk")) * mult * (-1 if neg else 1)
                    if val < min_net_assets: continue
                except: pass

            # Employee filter
            emp_s = fin.get("employees","")
            if (emp_min > 0 or emp_max > 0) and emp_s:
                try:
                    e = int(emp_s)
                    if emp_min > 0 and e < emp_min: continue
                    if emp_max > 0 and e > emp_max: continue
                except: pass

            results.append(c)

        # Sort by score then net assets
        def sort_key(c):
            fin = financials_cache.get(c.get("company_number",""),{})
            score = calc_score(fin)
            try:
                na_s = fin.get("net_assets","").replace("\u00a3","").replace(",","").strip()
                neg = na_s.startswith("-"); na_s = na_s.lstrip("-")
                mult = 1_000_000 if na_s.endswith("m") else (1_000 if na_s.endswith("k") else 1)
                na = float(na_s.rstrip("mk")) * mult * (-1 if neg else 1)
            except: na = -999999
            return (score, na)

        results.sort(key=sort_key, reverse=True)

        st.session_state.results = results
        st.session_state.director_cache = director_cache
        st.session_state.financials_cache = financials_cache
        st.session_state.search_criteria = {
            "location": location, "industries": ", ".join(selected_sic_labels),
            "fetch_financials": fetch_financials_flag,
            "min_net_assets": min_net_assets, "min_age": min_age,
            "total_results": len(results), "export_date": date.today().strftime("%d %B %Y")
        }

        progress_bar.progress(100)
        status_text.write(f"\u2705 **Complete** — {len(results):,} results ready")

        # Auto-send email if configured
        if send_email and email_to and email_configured:
            try:
                # Build Excel buffer
                from openpyxl import Workbook
                from openpyxl.styles import Font, PatternFill, Alignment
                from openpyxl.utils import get_column_letter
                from collections import Counter
                import pandas as pd
                # Build df for email (same as display)
                email_rows = []
                for c in results:
                    num = c.get("company_number","")
                    company_name = title_case_company(c.get("company_name", c.get("title","")))
                    addr = c.get("registered_office_address",{})
                    addr_str = " ".join(filter(None,[addr.get("address_line_1",""), addr.get("locality",""), addr.get("postal_code","")]))
                    sics = "; ".join(c.get("sic_codes",[]))
                    inc = c.get("date_of_creation","")
                    age = ""
                    if inc:
                        try:
                            y,m2,d2 = inc.split("-")
                            age = str((date.today()-date(int(y),int(m2),int(d2))).days//365)
                        except: pass
                    fin = financials_cache.get(num,{})
                    score = calc_score(fin)
                    score_str = "★" * min(score,5) if score > 0 else "☆"
                    dirs = director_cache.get(num,[])
                    if one_per_company and dirs: dirs = dirs[:1]
                    rows_data = dirs if dirs else [None]
                    category = ", ".join([l.split("(")[0].strip() for l in selected_sic_labels if any(s in sics for s in [sic_codes[sic_labels.index(l)]])]) or ", ".join(selected_sic_labels[:1]).split("(")[0].strip()
                    for o in rows_data:
                        name = ""
                        appt = ""
                        if o:
                            name = " ".join(reversed([p.strip() for p in o.get("name","").split(",")]))
                            appt = o.get("appointed_on","")
                        first_n, last_n = split_director_name(name)
                        ch_url = f"https://find-and-update.company-information.service.gov.uk/company/{num}"
                        li_url = "https://www.linkedin.com/search/results/people/?keywords=" + requests.utils.quote(f"{first_n} {last_n} {company_name}")
                        email_rows.append({
                            "Score": score_str, "First Name": first_n, "Surname": last_n,
                            "Company": company_name, "Number": num, "Address": addr_str,
                            "SIC": sics, "Category": category, "Incorporated": inc, "Age": age,
                            "Total Assets": fin.get("total_assets",""), "Net Assets": fin.get("net_assets",""),
                            "Fixed Assets": fin.get("fixed_assets",""), "Current Assets": fin.get("current_assets",""),
                            "Employees": fin.get("employees",""), "Accounts Date": fin.get("accounts_date",""),
                            "Dir. Appointed": appt, "Accountant": fin.get("accountant",""),
                            "CH Link": ch_url, "LinkedIn": li_url,
                        })
                email_df = pd.DataFrame(email_rows)

                # Build Excel
                wb = Workbook()
                ws = wb.active; ws.title = "Prospects"
                base_cols = [c for c in email_df.columns if c not in ["CH Link","LinkedIn"]]
                headers_xl = base_cols + ["CH company","Officers","LinkedIn"]
                hdr_fill = PatternFill("solid", fgColor="1a4a2e")
                for i, h in enumerate(headers_xl, 1):
                    cell = ws.cell(row=1, column=i, value=h)
                    cell.fill = hdr_fill
                    cell.font = Font(name="Arial", color="FFFFFF", bold=True, size=10)
                    cell.alignment = Alignment(horizontal="center", wrap_text=True)
                ws.row_dimensions[1].height = 30
                fill_even = PatternFill("solid", fgColor="EBF3FB")
                fill_odd = PatternFill("solid", fgColor="FFFFFF")
                for rn, (_, row) in enumerate(email_df.iterrows(), 2):
                    fill = fill_even if rn % 2 == 0 else fill_odd
                    row_vals = [row[c] for c in base_cols] + [row["CH Link"], f"{row['CH Link']}/officers", row["LinkedIn"]]
                    for ci, val in enumerate(row_vals, 1):
                        cell = ws.cell(row=rn, column=ci, value=val)
                        cell.fill = fill
                        cell.font = Font(name="Arial", size=9)
                        cell.alignment = Alignment(horizontal="left", vertical="center")
                        if ci > len(base_cols):
                            labels = ["Open","Officers","LinkedIn"]
                            cell.value = labels[ci-len(base_cols)-1]
                            cell.hyperlink = val
                            cell.font = Font(name="Arial", size=9, color="0563C1", underline="single")
                for ci, h in enumerate(headers_xl, 1):
                    col_letter = get_column_letter(ci)
                    max_len = len(str(h))
                    for rn in range(2, ws.max_row+1):
                        v = ws.cell(row=rn, column=ci).value
                        if v: max_len = max(max_len, len(str(v)))
                    ws.column_dimensions[col_letter].width = min(max(max_len+2, 8), 40)
                ws.auto_filter.ref = ws.dimensions
                ws.freeze_panes = "A2"
                # Accountants sheet
                ws_acct = wb.create_sheet("Accountants")
                acct_counts = Counter(r for r in email_df["Accountant"].tolist() if r and str(r).strip())
                for ci, h in enumerate(["Accountant Firm","No. of Clients","Companies"], 1):
                    cell = ws_acct.cell(row=1, column=ci, value=h)
                    cell.fill = PatternFill("solid", fgColor="1a4a2e")
                    cell.font = Font(bold=True, name="Arial", size=10, color="FFFFFF")
                acct_companies = {}
                for _, row in email_df.iterrows():
                    acct = str(row.get("Accountant","")).strip()
                    if acct:
                        acct_companies.setdefault(acct, [])
                        co = str(row.get("Company","")).strip()
                        if co and co not in acct_companies[acct]: acct_companies[acct].append(co)
                for rn, (acct, count) in enumerate(acct_counts.most_common(), 2):
                    ws_acct.cell(row=rn, column=1, value=acct).font = Font(name="Arial", size=9)
                    ws_acct.cell(row=rn, column=2, value=count).font = Font(name="Arial", size=9)
                    ws_acct.cell(row=rn, column=3, value=", ".join(acct_companies.get(acct,[]))).font = Font(name="Arial", size=9)
                ws_acct.column_dimensions["A"].width = 40
                ws_acct.column_dimensions["B"].width = 14
                ws_acct.column_dimensions["C"].width = 60
                # Criteria sheet
                ws2 = wb.create_sheet("Search Criteria")
                crit = st.session_state.get("search_criteria",{})
                for i, (k,v) in enumerate(crit.items(), 1):
                    ws2.cell(row=i, column=1, value=k).font = Font(bold=True, name="Arial")
                    ws2.cell(row=i, column=2, value=str(v)).font = Font(name="Arial")
                xl_buf = io.BytesIO(); wb.save(xl_buf); xl_buf.seek(0)

                # Build CSV
                csv_df = email_df.copy()
                csv_df["CH company"] = email_df["CH Link"]
                csv_df["Officers"] = email_df["CH Link"].apply(lambda x: x+"/officers")
                csv_df["LinkedIn search"] = email_df["LinkedIn"]
                csv_df = csv_df.drop(columns=["CH Link","LinkedIn"])
                csv_str = csv_df.to_csv(index=False)

                search_date_str = date.today().strftime("%d%m%y")
                result = send_email_results(
                    gmail_user, gmail_pass, email_to,
                    xl_buf.getvalue(), csv_str,
                    date.today().strftime("%d %B %Y"),
                    st.session_state.get("search_criteria",{})
                )
                if result is True:
                    st.success(f"✅ Results emailed to {email_to}")
                    update_job(completed=True, email_sent=True, running=False)
                else:
                    st.warning(f"⚠️ Email failed: {result}")
                    update_job(completed=True, email_sent=False, running=False)
            except Exception as e:
                st.warning(f"⚠️ Email error: {e}")

# ─── Background job status ───────────────────────────────────────────────────
job = get_job()
if job["running"]:
    elapsed_s = int(time.time() - job["started_at"]) if job["started_at"] else 0
    elapsed_str = f"{elapsed_s//60}m {elapsed_s%60}s" if elapsed_s >= 60 else f"{elapsed_s}s"
    st.info(
        f"🔄 **Search running in background** — {job['search_params']}\n\n"
        f"Directors: {job['dir_done']:,}/{job['total']:,} | "
        f"Financials: {job['fin_done']:,}/{job['total']:,} | "
        f"Elapsed: {elapsed_str}\n\n"
        f"You can close this browser — results will be emailed when complete."
    )
    time.sleep(3)
    st.rerun()
elif job["completed"] and not job["running"]:
    if job["email_sent"]:
        st.success("✅ Last search completed — results have been emailed.")
    else:
        st.warning("⚠️ Last search completed but email failed. Check your inbox or try again.")

# ─── Results display ──────────────────────────────────────────────────────────
results = st.session_state.get("results",[])
director_cache = st.session_state.get("director_cache",{})
financials_cache = st.session_state.get("financials_cache",{})

if results:
    st.markdown(f"### {len(results):,} results loaded")

    # Build rows for display
    rows = []
    for c in results:
        num = c.get("company_number","")
        company_name = title_case_company(c.get("company_name", c.get("title","")))
        addr = c.get("registered_office_address",{})
        addr_str = " ".join(filter(None,[addr.get("address_line_1",""),
                                          addr.get("locality",""), addr.get("postal_code","")]))
        sics = "; ".join(c.get("sic_codes",[]))
        inc = c.get("date_of_creation","")
        age = ""
        if inc:
            try:
                y,m2,d2 = inc.split("-")
                age = str((date.today()-date(int(y),int(m2),int(d2))).days//365)
            except: pass
        fin = financials_cache.get(num,{})
        score = calc_score(fin)
        score_str = "\u2605" * min(score,5) if score > 0 else "\u2606"
        dirs = director_cache.get(num,[])
        if one_per_company and dirs: dirs = dirs[:1]
        rows_data = dirs if dirs else [None]
        for o in rows_data:
            name = ""
            appt = ""
            if o:
                name = " ".join(reversed([p.strip() for p in o.get("name","").split(",")]))
                appt = o.get("appointed_on","")
            first_n, last_n = split_director_name(name)
            ch_url = f"https://find-and-update.company-information.service.gov.uk/company/{num}"
            li_url = "https://www.linkedin.com/search/results/people/?keywords=" + requests.utils.quote(f"{first_n} {last_n} {company_name}")
            rows.append({
                "Score": score_str,
                "First Name": first_n,
                "Surname": last_n,
                "Company": company_name,
                "Number": num,
                "Address": addr_str,
                "SIC": sics,
                "Category": ", ".join([l.split("(")[0].strip() for l in selected_sic_labels if any(s in sics for s in [sic_codes[sic_labels.index(l)]])]) or ", ".join(selected_sic_labels[:1]).split("(")[0].strip(),
                "Incorporated": inc,
                "Age": age,
                "Total Assets": fin.get("total_assets",""),
                "Net Assets": fin.get("net_assets",""),
                "Fixed Assets": fin.get("fixed_assets",""),
                "Current Assets": fin.get("current_assets",""),
                "Employees": fin.get("employees",""),
                "Accounts Date": fin.get("accounts_date",""),
                "Dir. Appointed": appt,
                "Accountant": fin.get("accountant",""),
                "CH Link": ch_url,
                "LinkedIn": li_url,
            })

    import pandas as pd
    df = pd.DataFrame(rows)
    # Sort by numeric score descending
    if "_score_num" in df.columns:
        df = df.sort_values("_score_num", ascending=False).reset_index(drop=True)
        df = df.drop(columns=["_score_num"])

    # Display table
    display_df = df.copy()
    display_df["CH company"] = display_df["CH Link"]
    display_df["Officers"] = display_df["CH Link"].apply(lambda x: x + "/officers" if x else "")
    display_df = display_df.drop(columns=["CH Link"])

    st.dataframe(
        display_df,
        use_container_width=True,
        height=600,
        hide_index=True,
        column_config={
            "Score": st.column_config.TextColumn("★ Score", width=80),
            "First Name": st.column_config.TextColumn("First Name", width=100),
            "Surname": st.column_config.TextColumn("Surname", width=110),
            "Company": st.column_config.TextColumn("Company", width=200),
            "Number": st.column_config.TextColumn("Number", width=90),
            "Address": st.column_config.TextColumn("Address", width=250),
            "SIC": st.column_config.TextColumn("SIC", width=80),
            "Category": st.column_config.TextColumn("Category", width=150),
            "Incorporated": st.column_config.TextColumn("Incorporated", width=100),
            "Age": st.column_config.TextColumn("Age", width=50),
            "Total Assets": st.column_config.TextColumn("Total Assets", width=100),
            "Net Assets": st.column_config.TextColumn("Net Assets", width=100),
            "Fixed Assets": st.column_config.TextColumn("Fixed Assets", width=100),
            "Current Assets": st.column_config.TextColumn("Current Assets", width=110),
            "Employees": st.column_config.TextColumn("Employees", width=85),
            "Accounts Date": st.column_config.TextColumn("Accounts Date", width=110),
            "Dir. Appointed": st.column_config.TextColumn("Dir. Appointed", width=110),
            "Accountant": st.column_config.TextColumn("Accountant", width=180),
            "CH company": st.column_config.LinkColumn("CH Company", width=90, display_text="Open"),
            "Officers": st.column_config.LinkColumn("Officers", width=80, display_text="Officers"),
            "LinkedIn": st.column_config.LinkColumn("LinkedIn", width=80, display_text="Search"),
        }
    )

    # Build smart filename
    def _make_filename(ext):
        loc = location.strip().replace(" ","").lower()[:10]
        cats = "_".join([
            l.split("(")[0].strip().replace(" ","").lower()[:8]
            for l in selected_sic_labels[:2]
        ]) if selected_sic_labels else "all"
        cats = cats[:20]
        d = date.today().strftime("%d%m%y")
        return f"{loc}_{cats}_{d}.{ext}"

    # Export buttons
    st.markdown("---")
    col_a, col_b, col_c = st.columns([1,1,2])

    with col_a:
        # Excel export
        try:
            from openpyxl import Workbook
            from openpyxl.styles import Font, PatternFill, Alignment
            from openpyxl.utils import get_column_letter
            from collections import Counter
            wb = Workbook()
            ws = wb.active
            ws.title = "Prospects"
            # Build headers
            base_cols = [c for c in df.columns if c not in ["CH Link", "LinkedIn"]]
            headers_xl = base_cols + ["CH company", "Officers", "LinkedIn"]
            hdr_fill = PatternFill("solid", fgColor="1a4a2e")
            hdr_font = Font(name="Arial", color="FFFFFF", bold=True, size=10)
            for i, h in enumerate(headers_xl, 1):
                cell = ws.cell(row=1, column=i, value=h)
                cell.fill = hdr_fill
                cell.font = hdr_font
                cell.alignment = Alignment(horizontal="center", wrap_text=True)
            ws.row_dimensions[1].height = 30
            fill_even = PatternFill("solid", fgColor="EBF3FB")
            fill_odd  = PatternFill("solid", fgColor="FFFFFF")
            for rn, (_, row) in enumerate(df.iterrows(), 2):
                fill = fill_even if rn % 2 == 0 else fill_odd
                row_vals = [row[c] for c in base_cols] + [row["CH Link"], f"{row['CH Link']}/officers", row["LinkedIn"]]
                for ci, val in enumerate(row_vals, 1):
                    cell = ws.cell(row=rn, column=ci, value=val)
                    cell.fill = fill
                    cell.font = Font(name="Arial", size=9)
                    cell.alignment = Alignment(horizontal="left", vertical="center")
                    if ci > len(base_cols):
                        labels = ["Open", "Officers", "LinkedIn"]
                        cell.value = labels[ci - len(base_cols) - 1]
                        cell.hyperlink = val
                        cell.font = Font(name="Arial", size=9, color="0563C1", underline="single")
            # Auto-fit columns
            for ci, h in enumerate(headers_xl, 1):
                col_letter = get_column_letter(ci)
                max_len = len(str(h))
                for rn in range(2, ws.max_row + 1):
                    v = ws.cell(row=rn, column=ci).value
                    if v: max_len = max(max_len, len(str(v)))
                ws.column_dimensions[col_letter].width = min(max(max_len + 2, 8), 40)
            ws.auto_filter.ref = ws.dimensions
            ws.freeze_panes = "A2"

            # Accountants summary sheet
            ws_acct = wb.create_sheet("Accountants")
            acct_counts = Counter(
                r for r in df["Accountant"].tolist() if r and str(r).strip()
            )
            ws_acct.cell(row=1, column=1, value="Accountant Firm").font = Font(bold=True, name="Arial", size=10)
            ws_acct.cell(row=1, column=2, value="No. of Clients").font = Font(bold=True, name="Arial", size=10)
            ws_acct.cell(row=1, column=3, value="Companies").font = Font(bold=True, name="Arial", size=10)
            ws_acct.row_dimensions[1].height = 20
            for cell in ws_acct[1]:
                cell.fill = PatternFill("solid", fgColor="1a4a2e")
                cell.font = Font(bold=True, name="Arial", size=10, color="FFFFFF")
                cell.alignment = Alignment(horizontal="center")
            # For each accountant, list which companies they appear against
            acct_companies = {}
            for _, row in df.iterrows():
                acct = str(row.get("Accountant","")).strip()
                if acct:
                    acct_companies.setdefault(acct, [])
                    co = str(row.get("Company","")).strip()
                    if co and co not in acct_companies[acct]:
                        acct_companies[acct].append(co)
            for rn, (acct, count) in enumerate(acct_counts.most_common(), 2):
                fill = PatternFill("solid", fgColor="EBF3FB") if rn % 2 == 0 else PatternFill("solid", fgColor="FFFFFF")
                ws_acct.cell(row=rn, column=1, value=acct).font = Font(name="Arial", size=9)
                ws_acct.cell(row=rn, column=2, value=count).font = Font(name="Arial", size=9)
                ws_acct.cell(row=rn, column=3, value=", ".join(acct_companies.get(acct,[]))).font = Font(name="Arial", size=9)
                for ci in range(1, 4):
                    ws_acct.cell(row=rn, column=ci).fill = fill
            ws_acct.column_dimensions["A"].width = 40
            ws_acct.column_dimensions["B"].width = 14
            ws_acct.column_dimensions["C"].width = 60
            ws_acct.auto_filter.ref = f"A1:C{ws_acct.max_row}"

            # Criteria sheet
            ws2 = wb.create_sheet("Search Criteria")
            crit = st.session_state.get("search_criteria",{})
            for i, (k,v) in enumerate(crit.items(), 1):
                ws2.cell(row=i, column=1, value=k).font = Font(bold=True, name="Arial")
                ws2.cell(row=i, column=2, value=str(v)).font = Font(name="Arial")
            ws2.column_dimensions["A"].width = 22
            ws2.column_dimensions["B"].width = 50

            buf = io.BytesIO()
            wb.save(buf)
            buf.seek(0)
            st.download_button(
                "\u2b07 Download Excel",
                data=buf.getvalue(),
                file_name=_make_filename("xlsx"),
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        except Exception as e:
            st.error(f"Excel error: {e}")

    with col_b:
        # CSV export
        csv_buf = io.StringIO()
        df_csv = df.copy()
        df_csv["CH company"] = df["CH Link"]
        df_csv["Officers"] = df["CH Link"].apply(lambda x: x + "/officers")
        df_csv["LinkedIn search"] = df["LinkedIn"]
        df_csv = df_csv.drop(columns=["CH Link","LinkedIn"])
        csv_data = df_csv.to_csv(index=False)
        st.download_button(
            "\u2b07 Download CSV",
            data=csv_data.encode("utf-8-sig"),
            file_name=_make_filename("csv"),
            mime="text/csv",
            use_container_width=True
        )

else:
    st.info("Configure your filters in the sidebar and click **Search Companies House** to begin.")

# ─── Error log display ────────────────────────────────────────────────────────
if _log_buffer:
    with st.expander(f"🔍 Event log ({len(_log_buffer)} entries)", expanded=False):
        st.code(get_log_text(), language=None)
        if st.button("Clear log"):
            _log_buffer.clear()
    st.markdown("""
    **Tips:**
    - Select one or more industries from the sidebar
    - Enter a UK location (town, county or postcode area)
    - Tick **Fetch financials** for net assets, employees and scoring
    - Results are sorted by quality score (\u2605\u2605\u2605\u2605\u2605 = best prospects)
    """)
