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
    ("Architecture (71112)", "71112"),
    ("Auditing (69202)", "69202"),
    ("Building construction (41202)", "41202"),
    ("Car dealerships (45111)", "45111"),
    ("Car repairs (45200)", "45200"),
    ("Care homes (87100)", "87100"),
    ("Cleaning services (81210)", "81210"),
    ("Clothing retail (47710)", "47710"),
    ("Courier services (53200)", "53200"),
    ("Dental practices (86220)", "86220"),
    ("Domiciliary care (88100)", "88100"),
    ("Electrical contracting (43210)", "43210"),
    ("Engineering consultancy (71121)", "71121"),
    ("Estate agents (68310)", "68310"),
    ("Event management - conferences (82302)", "82302"),
    ("Event management - exhibitions (82301)", "82301"),
    ("Financial services nec (64999)", "64999"),
    ("Fitness / gyms (93130)", "93130"),
    ("Food manufacturing (10000)", "10000"),
    ("Funeral services (96030)", "96030"),
    ("GP practices (86210)", "86210"),
    ("Hairdressing & beauty (96020)", "96020"),
    ("HR consulting (70229)", "70229"),
    ("Independent schools (85310)", "85310"),
    ("Insurance brokers (66220)", "66220"),
    ("Investment management (64300)", "64300"),
    ("IT consultancy (62020)", "62020"),
    ("Jewellery retail (47770)", "47770"),
    ("Landscaping (81300)", "81300"),
    ("Management consulting (70229)", "70229"),
    ("Market research (73200)", "73200"),
    ("Metal fabrication (25110)", "25110"),
    ("Mortgage brokers (66190)", "66190"),
    ("Opticians (86230)", "86230"),
    ("Painting & decorating (43341)", "43341"),
    ("Photography (74201)", "74201"),
    ("Physiotherapy (86901)", "86901"),
    ("Plumbing & heating (43220)", "43220"),
    ("PR / communications (70210)", "70210"),
    ("Pre-school / nursery (85100)", "85100"),
    ("Printing (18120)", "18120"),
    ("Private hospitals (86101)", "86101"),
    ("Property buying & selling (68100)", "68100"),
    ("Property development (41100)", "41100"),
    ("Property management (68320)", "68320"),
    ("Pubs / wine bars (56302)", "56302"),
    ("Recruitment (78200)", "78200"),
    ("Restaurants (56101)", "56101"),
    ("Road freight (49410)", "49410"),
    ("Roofing (43910)", "43910"),
    ("Security services (80100)", "80100"),
    ("Software development (62012)", "62012"),
    ("Solicitors (69102)", "69102"),
    ("Supermarkets (47110)", "47110"),
    ("Surveying (71112)", "71112"),
    ("Takeaways (56102)", "56102"),
    ("Tax consulting (69203)", "69203"),
    ("Tutoring (85590)", "85590"),
    ("Veterinary (75000)", "75000"),
    ("Vocational training (85320)", "85320"),
    ("Warehousing (52100)", "52100"),
    ("Web design (62090)", "62090"),
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
              "fixed_assets":"","current_assets":"","employees":""}
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

# ─── Streamlit UI ─────────────────────────────────────────────────────────────

st.markdown('<div class="main-header">\U0001f3e2 Companies House Prospector</div>', unsafe_allow_html=True)

# ─── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### \U0001f511 API Key")
    api_key = st.text_input("Companies House API Key", type="password",
                             value=st.session_state.get("api_key",""),
                             placeholder="Paste your API key here")
    if api_key: st.session_state["api_key"] = api_key

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

    st.markdown("### \U0001f4c5 Incorporation Date")
    col1, col2 = st.columns(2)
    with col1: inc_after = st.text_input("After (year)", placeholder="2000")
    with col2: inc_before = st.text_input("Before (year)", placeholder="2022")

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
        if inc_after.strip(): base_params["incorporated_from"] = f"{inc_after.strip()}-01-01"
        if inc_before.strip(): base_params["incorporated_to"] = f"{inc_before.strip()}-12-31"

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
            except: return num, []

        def fetch_fin(c):
            num = c.get("company_number","")
            if not num: return num, {}
            return num, fetch_financials(num, api_key)

        def run_dirs():
            with ThreadPoolExecutor(max_workers=6) as ex:
                for future in as_completed({ex.submit(fetch_dir,c):c for c in all_items}):
                    try:
                        num, active = future.result()
                        with dir_lock: director_cache[num] = active
                    except: pass
                    with dir_lock: dir_done[0] += 1

        def run_fins():
            if not fetch_financials_flag: return
            with ThreadPoolExecutor(max_workers=3) as ex:
                for future in as_completed({ex.submit(fetch_fin,c):c for c in all_items}):
                    try:
                        num, fin = future.result()
                        with fin_lock: financials_cache[num] = fin
                    except: pass
                    with fin_lock: fin_done[0] += 1

        t1 = threading.Thread(target=run_dirs, daemon=True)
        t2 = threading.Thread(target=run_fins, daemon=True)
        t1.start(); t2.start()

        # Show live progress while threads run
        prog_placeholder = st.empty()
        while t1.is_alive() or t2.is_alive():
            d = dir_done[0]; f = fin_done[0]
            prog_placeholder.markdown(
                f"**Directors:** {d:,}/{total:,} &nbsp;&nbsp; "
                f"**Financials:** {f:,}/{total:,} &nbsp;&nbsp; "
                f"**API calls:** {_rate_limiter.calls_in_window()}/575"
                + (" \u23f8 *Rate limit - waiting...*" if _rate_limiter.paused else "")
            )
            overall = 30 + int(((d + f) / (total * 2)) * 55)
            progress_bar.progress(min(overall, 85))
            time.sleep(1)

        t1.join(); t2.join()
        prog_placeholder.empty()

        # ── Stage 4: Filter, score, sort ──────────────────────────────────────
        status_text.write("**Stage 4/4** — Filtering and scoring results...")
        progress_bar.progress(90)

        results = []
        for c in all_items:
            num = c.get("company_number","")
            fin = financials_cache.get(num,{})

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
                "Incorporated": inc,
                "Age": age,
                "Total Assets": fin.get("total_assets",""),
                "Net Assets": fin.get("net_assets",""),
                "Fixed Assets": fin.get("fixed_assets",""),
                "Current Assets": fin.get("current_assets",""),
                "Employees": fin.get("employees",""),
                "Accounts Date": fin.get("accounts_date",""),
                "Dir. Appointed": appt,
                "CH Link": ch_url,
                "LinkedIn": li_url,
            })

    import pandas as pd
    df = pd.DataFrame(rows)

    # Display table
    st.dataframe(
        df.drop(columns=["CH Link","LinkedIn"]),
        use_container_width=True,
        height=500,
        column_config={
            "Score": st.column_config.TextColumn("Score", width="small"),
            "First Name": st.column_config.TextColumn(width="small"),
            "Surname": st.column_config.TextColumn(width="small"),
            "Company": st.column_config.TextColumn(width="medium"),
            "Number": st.column_config.TextColumn(width="small"),
            "Address": st.column_config.TextColumn(width="large"),
            "SIC": st.column_config.TextColumn(width="small"),
            "Age": st.column_config.TextColumn(width="small"),
            "Total Assets": st.column_config.TextColumn(width="small"),
            "Net Assets": st.column_config.TextColumn(width="small"),
            "Employees": st.column_config.TextColumn(width="small"),
            "Accounts Date": st.column_config.TextColumn(width="small"),
            "Dir. Appointed": st.column_config.TextColumn(width="small"),
        }
    )

    # Export buttons
    st.markdown("---")
    col_a, col_b, col_c = st.columns([1,1,2])

    with col_a:
        # Excel export
        try:
            from openpyxl import Workbook
            from openpyxl.styles import Font, PatternFill, Alignment
            from openpyxl.utils import get_column_letter
            from openpyxl.worksheet.table import Table, TableStyleInfo
            wb = Workbook()
            ws = wb.active
            ws.title = "Prospects"
            headers_xl = list(df.columns) + ["CH company", "Officers", "LinkedIn"]
            hdr_fill = PatternFill("solid", fgColor="1a4a2e")
            hdr_font = Font(name="Arial", color="FFFFFF", bold=True, size=10)
            for i, h in enumerate(headers_xl, 1):
                cell = ws.cell(row=1, column=i, value=h)
                cell.fill = hdr_fill
                cell.font = hdr_font
                cell.alignment = Alignment(horizontal="center")
            fill_even = PatternFill("solid", fgColor="F5F9F5")
            fill_odd  = PatternFill("solid", fgColor="FFFFFF")
            for rn, (_, row) in enumerate(df.iterrows(), 2):
                fill = fill_even if rn % 2 == 0 else fill_odd
                for ci, val in enumerate(list(row) + [row["CH Link"], f"{row['CH Link']}/officers", row["LinkedIn"]], 1):
                    cell = ws.cell(row=rn, column=ci, value=val)
                    cell.fill = fill
                    cell.font = Font(name="Arial", size=9)
                    cell.alignment = Alignment(horizontal="left")
                    if ci > len(df.columns):
                        labels = ["Open", "Officers", "LinkedIn"]
                        cell.value = labels[ci - len(df.columns) - 1]
                        cell.hyperlink = val
                        cell.font = Font(name="Arial", size=9, color="0563C1", underline="single")
            last_col = get_column_letter(len(headers_xl))
            table = Table(displayName="Prospects", ref=f"A1:{last_col}{len(df)+1}")
            table.tableStyleInfo = TableStyleInfo(name="TableStyleMedium2", showRowStripes=True)
            ws.add_table(table)
            ws.freeze_panes = "A2"
            # Criteria sheet
            ws2 = wb.create_sheet("Search Criteria")
            crit = st.session_state.get("search_criteria",{})
            for i, (k,v) in enumerate(crit.items(), 1):
                ws2.cell(row=i, column=1, value=k).font = Font(bold=True)
                ws2.cell(row=i, column=2, value=str(v))
            buf = io.BytesIO()
            wb.save(buf)
            buf.seek(0)
            st.download_button(
                "\u2b07 Download Excel",
                data=buf.getvalue(),
                file_name=f"prospects_{date.today()}.xlsx",
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
        df_csv = df_csv.drop(columns=["CH Link","LinkedIn"])
        csv_data = df_csv.to_csv(index=False)
        st.download_button(
            "\u2b07 Download CSV",
            data=csv_data.encode("utf-8-sig"),
            file_name=f"prospects_{date.today()}.csv",
            mime="text/csv",
            use_container_width=True
        )

else:
    st.info("Configure your filters in the sidebar and click **Search Companies House** to begin.")
    st.markdown("""
    **Tips:**
    - Select one or more industries from the sidebar
    - Enter a UK location (town, county or postcode area)
    - Tick **Fetch financials** for net assets, employees and scoring
    - Results are sorted by quality score (\u2605\u2605\u2605\u2605\u2605 = best prospects)
    """)
