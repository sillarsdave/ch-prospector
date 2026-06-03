# -*- coding: utf-8 -*-
"""
Background worker — runs permanently on Railway.
Picks up search jobs from Redis, runs them, saves results back to Redis.
Streamlit then sends the email (it has outbound network access).
"""
import json
import os
import time
import threading
import re
import io
import requests
import traceback
import redis
from base64 import b64encode
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")

def get_redis():
    return redis.from_url(REDIS_URL, decode_responses=True)

def is_cancelled(job_id):
    """Check if a cancel has been requested for this job."""
    try:
        r = get_redis()
        return r.get("ch_cancel") == job_id
    except:
        return False

API_BASE = "https://api.company-information.service.gov.uk"

class RateLimiter:
    def __init__(self, max_calls=575, window=300):
        self.max_calls = max_calls
        self.window = window
        self.calls = deque()
        self.lock = threading.Lock()

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
                self.record_call()
                return
            with self.lock:
                wait_time = (self.calls[0] + self.window) - time.time() + 0.1 if self.calls else 1
            time.sleep(max(0.1, wait_time))

_rl = RateLimiter()

def ch_get(path, api_key):
    _rl.wait_if_needed()
    auth = "Basic " + b64encode(f"{api_key}:".encode()).decode()
    r = requests.get(API_BASE + path, headers={"Authorization": auth}, timeout=20)
    if r.status_code == 429:
        time.sleep(5)
        _rl.wait_if_needed()
        r = requests.get(API_BASE + path, headers={"Authorization": auth}, timeout=20)
    r.raise_for_status()
    return r.json()

def fmt_currency(val):
    if val is None: return ""
    try:
        v = float(val)
        if abs(v) >= 1_000_000: return f"£{v/1_000_000:.1f}m"
        elif abs(v) >= 1_000:   return f"£{v/1_000:.0f}k"
        else:                    return f"£{v:.0f}"
    except: return ""

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
    caps  = [p for p in parts if p.replace("-","").isupper() and len(p) > 1]
    lower = [p for p in parts if not p.replace("-","").isupper()]
    if caps:
        surname = tc(caps[-1])
        first   = tc(lower[0]) if lower else tc(parts[0])
    else:
        first   = tc(parts[0])
        surname = tc(parts[-1]) if len(parts) > 1 else ""
    return first, surname

def fetch_financials(company_number, api_key):
    from bs4 import BeautifulSoup
    result = {"accounts_date":"","total_assets":"","net_assets":"",
              "fixed_assets":"","current_assets":"","employees":"","accountant":""}
    try:
        auth = "Basic " + b64encode(f"{api_key}:".encode()).decode()
        headers = {"Authorization": auth}
        _rl.wait_if_needed()
        fh = requests.get(f"{API_BASE}/company/{company_number}/filing-history",
                         params={"category":"accounts","items_per_page":10},
                         headers=headers, timeout=12)
        if fh.status_code == 429:
            time.sleep(3); _rl.wait_if_needed()
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
        _rl.wait_if_needed()
        dm = requests.get(doc_meta_url, headers=headers, timeout=12)
        if dm.status_code != 200: return result
        meta = dm.json()
        doc_url = meta.get("links",{}).get("document","")
        if not doc_url: return result
        if "application/xhtml+xml" not in meta.get("resources",{}): return result
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

        try:
            SUFFIXES = r"(?:LLP|Chartered Accountants|Certified Accountants|Chartered Certified Accountants|& Co(?:\.|mpany)?|Accountants)"
            trigger_pat = (r"(?:prepared by|statutory auditors?|reporting accountants?|"
                          r"independent auditors?|audited by|accounts? (?:have been )?prepared by)"
                          r"[:\s]+([A-Z][A-Za-z0-9 &,\.\-]{2,50}?" + SUFFIXES + r")")
            full_text = soup.get_text(separator=" ", strip=True)
            m = re.search(trigger_pat, full_text)
            if m:
                accountant = m.group(1).strip().rstrip(".,")
            else:
                fallback_pat = r"([A-Z][A-Za-z0-9 &,\.\-]{2,50}?" + SUFFIXES + r")"
                accountant = ""
                for m in re.finditer(fallback_pat, full_text):
                    candidate = m.group(1).strip().rstrip(".,")
                    skip = ["the company","the directors","companies house","hmrc",
                            "limited company","association of","institute of",
                            "liability partnership","recruitment","staffing",
                            "employment","personnel","limited liability"]
                    if any(s in candidate.lower() for s in skip): continue
                    if len(candidate) > 4:
                        accountant = candidate
                        break
            if accountant:
                accountant = accountant.split("|")[0].strip()
                for prefix in ["Pages For Filing With Registrar ","PAGES FOR FILING WITH REGISTRAR "]:
                    if accountant.startswith(prefix):
                        accountant = accountant[len(prefix):]
                accountant = accountant.strip()[:60]
            result["accountant"] = accountant
        except: pass
    except: pass
    return result

def calc_score(fin):
    score = 0
    def parse_val(s):
        if not s: return None
        try:
            s = s.replace("£","").replace(",","").strip()
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
    items = []; start = 0; total = None
    while True:
        params = {**params_base, "size": 100, "start_index": start}
        _rl.wait_if_needed()
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

def write_status(status):
    try:
        r = get_redis()
        r.set("ch_status", json.dumps(status))
    except Exception as e:
        print(f"[{datetime.now()}] Status write error: {e}")

def run_job(job):
    api_key        = os.environ.get("CH_API_KEY","")
    email_to       = job.get("email_to","")
    location       = job.get("location","Surrey")
    selected_sics  = job.get("sic_codes",[])
    sic_labels     = job.get("sic_labels",[])
    fetch_fin_flag = job.get("fetch_financials", True)
    min_age        = job.get("min_age", 3)
    max_age        = job.get("max_age", 0)
    excl_dormant   = job.get("excl_dormant", True)
    min_net_assets = job.get("min_net_assets", 0)
    emp_min        = job.get("emp_min", 0)
    emp_max        = job.get("emp_max", 0)
    one_per_co     = job.get("one_per_company", True)
    company_types  = job.get("company_types", ["ltd","llp"])

    # Clear any cancel flag from previous job
    try:
        get_redis().delete("ch_cancel")
    except: pass

    write_status({"running": True, "stage": "Fetching companies...", "dir_done": 0,
                  "fin_done": 0, "total": 0, "started_at": time.time(), "error": None,
                  "job_id": job.get("job_id",""), "ready_to_email": False})
    try:
        base_params = {"location": location, "company_status": "active"}
        if company_types: base_params["company_type"] = ",".join(company_types)

        all_items = []; seen = set()
        for sic in selected_sics:
            fetched = fetch_all_for_sic(sic, base_params, api_key)
            for c in fetched:
                num = c.get("company_number","")
                if num and num not in seen:
                    seen.add(num); all_items.append(c)

        today = date.today()
        filtered = []
        for c in all_items:
            if excl_dormant and "dormant" in c.get("company_status","").lower(): continue
            inc = c.get("date_of_creation","")
            if inc:
                try:
                    y,m2,d2 = inc.split("-")
                    age_yrs = (today - date(int(y),int(m2),int(d2))).days // 365
                    if min_age > 0 and age_yrs < min_age: continue
                    if max_age > 0 and age_yrs > max_age: continue
                except: pass
            filtered.append(c)
        all_items = filtered
        total = len(all_items)

        write_status({"running": True, "stage": f"Loading directors and financials for {total:,} companies...",
                      "dir_done": 0, "fin_done": 0, "total": total,
                      "started_at": time.time(), "error": None, "ready_to_email": False})

        director_cache = {}; financials_cache = {}
        dir_lock = threading.Lock(); fin_lock = threading.Lock()
        dir_done = [0]; fin_done = [0]
        start_time = time.time()

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
                futures = {ex.submit(fetch_dir,c):c for c in all_items}
                for future in as_completed(futures):
                    if is_cancelled(job.get("job_id","")):
                        print(f"[{datetime.now()}] Job cancelled during directors fetch")
                        return
                    try:
                        num, active = future.result()
                        with dir_lock: director_cache[num] = active
                    except: pass
                    with dir_lock: dir_done[0] += 1
                    write_status({"running": True, "stage": "Loading directors and financials...",
                                  "dir_done": dir_done[0], "fin_done": fin_done[0],
                                  "total": total, "started_at": start_time,
                                  "job_id": job.get("job_id",""),
                                  "error": None, "ready_to_email": False})

        def run_fins():
            if not fetch_fin_flag: return
            with ThreadPoolExecutor(max_workers=3) as ex:
                futures = {ex.submit(fetch_fin,c):c for c in all_items}
                for future in as_completed(futures):
                    if is_cancelled(job.get("job_id","")):
                        print(f"[{datetime.now()}] Job cancelled during financials fetch")
                        return
                    try:
                        num, fin = future.result()
                        with fin_lock: financials_cache[num] = fin
                    except: pass
                    with fin_lock: fin_done[0] += 1

        t1 = threading.Thread(target=run_dirs, daemon=False)
        t2 = threading.Thread(target=run_fins, daemon=False)
        t1.start(); t2.start()
        t1.join(); t2.join()

        if is_cancelled(job.get("job_id","")):
            print(f"[{datetime.now()}] Job cancelled — skipping results and email")
            write_status({"running": False, "stage": "Cancelled", "job_id": job.get("job_id",""),
                          "error": None, "email_sent": False, "ready_to_email": False})
            return

        write_status({"running": True, "stage": "Building results...",
                      "dir_done": dir_done[0], "fin_done": fin_done[0],
                      "total": total, "started_at": start_time,
                      "job_id": job.get("job_id",""),
                      "error": None, "ready_to_email": False})

        def sort_key(c):
            fin = financials_cache.get(c.get("company_number",""),{})
            score = calc_score(fin)
            try:
                na_s = fin.get("net_assets","").replace("£","").replace(",","").strip()
                neg = na_s.startswith("-"); na_s = na_s.lstrip("-")
                mult = 1_000_000 if na_s.endswith("m") else (1_000 if na_s.endswith("k") else 1)
                na = float(na_s.rstrip("mk")) * mult * (-1 if neg else 1)
            except: na = -999999
            return (score, na)

        results = []
        for c in all_items:
            num = c.get("company_number","")
            fin = financials_cache.get(num,{})
            if excl_dormant and "dormant" in c.get("company_status","").lower(): continue
            if min_net_assets > 0 and fin.get("net_assets",""):
                try:
                    s = fin["net_assets"].replace("£","").replace(",","").strip()
                    neg = s.startswith("-"); s = s.lstrip("-")
                    mult = 1_000_000 if s.endswith("m") else (1_000 if s.endswith("k") else 1)
                    val = float(s.rstrip("mk")) * mult * (-1 if neg else 1)
                    if val < min_net_assets: continue
                except: pass
            emp_s = fin.get("employees","")
            if (emp_min > 0 or emp_max > 0) and emp_s:
                try:
                    e = int(emp_s)
                    if emp_min > 0 and e < emp_min: continue
                    if emp_max > 0 and e > emp_max: continue
                except: pass
            results.append(c)

        results.sort(key=sort_key, reverse=True)

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
                    age = str((today-date(int(y),int(m2),int(d2))).days//365)
                except: pass
            fin = financials_cache.get(num,{})
            score = calc_score(fin)
            score_str = "★" * min(score,5) if score > 0 else "☆"
            dirs = director_cache.get(num,[])
            if one_per_co and dirs: dirs = dirs[:1]
            rows_data = dirs if dirs else [None]
            category = ", ".join([l.split("(")[0].strip() for l in sic_labels
                                  if any(sic in sics for sic in [l.split("(")[-1].rstrip(")")])]) or ""
            for o in rows_data:
                name = appt = ""
                if o:
                    name = " ".join(reversed([p.strip() for p in o.get("name","").split(",")]))
                    appt = o.get("appointed_on","")
                first_n, last_n = split_director_name(name)
                ch_url = f"https://find-and-update.company-information.service.gov.uk/company/{num}"
                li_url = "https://www.linkedin.com/search/results/people/?keywords=" + requests.utils.quote(f"{first_n} {last_n} {linkedin_company_keyword(company_name)}")
                rows.append({
                    "Score": score_str, "First Name": first_n, "Surname": last_n,
                    "Company": company_name, "Number": num, "Address": addr_str,
                    "SIC": sics, "Category": category, "Incorporated": inc, "Age": age,
                    "Total Assets": fin.get("total_assets",""), "Net Assets": fin.get("net_assets",""),
                    "Fixed Assets": fin.get("fixed_assets",""), "Current Assets": fin.get("current_assets",""),
                    "Employees": fin.get("employees",""), "Accounts Date": fin.get("accounts_date",""),
                    "Dir. Appointed": appt, "Accountant": fin.get("accountant",""),
                    "CH Link": ch_url, "LinkedIn": li_url,
                })

        # Build Excel
        import pandas as pd
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment
        from openpyxl.utils import get_column_letter
        from collections import Counter
        import base64

        df = pd.DataFrame(rows)
        wb = Workbook(); ws = wb.active; ws.title = "Prospects"
        base_cols = [c for c in df.columns if c not in ["CH Link","LinkedIn"]]
        headers_xl = base_cols + ["CH company","Officers","LinkedIn"]
        hdr_fill = PatternFill("solid", fgColor="1a4a2e")
        for i, h in enumerate(headers_xl, 1):
            cell = ws.cell(row=1, column=i, value=h)
            cell.fill = hdr_fill
            cell.font = Font(name="Arial", color="FFFFFF", bold=True, size=10)
            cell.alignment = Alignment(horizontal="center", wrap_text=True)
        ws.row_dimensions[1].height = 30
        fill_even = PatternFill("solid", fgColor="EBF3FB")
        fill_odd  = PatternFill("solid", fgColor="FFFFFF")
        for rn, (_, row) in enumerate(df.iterrows(), 2):
            fill = fill_even if rn % 2 == 0 else fill_odd
            row_vals = [row[c] for c in base_cols] + [row["CH Link"], f"{row['CH Link']}/officers", row["LinkedIn"]]
            for ci, val in enumerate(row_vals, 1):
                cell = ws.cell(row=rn, column=ci, value=val)
                cell.fill = fill; cell.font = Font(name="Arial", size=9)
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

        ws_acct = wb.create_sheet("Accountants")
        acct_counts = Counter(r for r in df["Accountant"].tolist() if r and str(r).strip())
        for ci, h in enumerate(["Accountant Firm","No. of Clients","Companies"], 1):
            cell = ws_acct.cell(row=1, column=ci, value=h)
            cell.fill = PatternFill("solid", fgColor="1a4a2e")
            cell.font = Font(bold=True, name="Arial", size=10, color="FFFFFF")
        acct_companies = {}
        for _, row in df.iterrows():
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

        ws2 = wb.create_sheet("Search Criteria")
        criteria = {"Location": location, "Industries": ", ".join(sic_labels),
                    "Total results": len(rows), "Export date": today.strftime("%d %B %Y")}
        for i, (k,v) in enumerate(criteria.items(), 1):
            ws2.cell(row=i, column=1, value=k).font = Font(bold=True, name="Arial")
            ws2.cell(row=i, column=2, value=str(v)).font = Font(name="Arial")

        xl_buf = io.BytesIO(); wb.save(xl_buf); xl_buf.seek(0)

        # Build CSV
        csv_df = df.copy()
        csv_df["CH company"] = df["CH Link"]
        csv_df["Officers"] = df["CH Link"].apply(lambda x: x+"/officers")
        csv_df["LinkedIn search"] = df["LinkedIn"]
        csv_df = csv_df.drop(columns=["CH Link","LinkedIn"])
        csv_str = csv_df.to_csv(index=False)

        # Send email via SendGrid (HTTPS - works on Railway)
        search_date = today.strftime("%d %B %Y")
        criteria = {"Location": location, "Industries": ", ".join(sic_labels),
                    "Total results": len(rows), "Export date": search_date}

        sg_key = os.environ.get("SENDGRID_API_KEY", "")
        from_email = "sillarsdave@gmail.com"

        try:
            import sendgrid as sg_module
            from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition

            body_lines = ["Your Companies House Prospector search has completed.", ""]
            for k, v in criteria.items():
                body_lines.append(f"{k}: {v}")
            body_lines.append("")
            body_lines.append("Please find the Excel and CSV results attached.")

            body_text = "\n".join(body_lines)
            message = Mail(
                from_email=from_email,
                to_emails=email_to,
                subject=f"Companies House Prospector Results — {search_date}",
                plain_text_content=body_text
            )

            encoded_xl = base64.b64encode(xl_buf.getvalue()).decode()
            message.attachment = Attachment(
                FileContent(encoded_xl),
                FileName(f"prospector_results_{search_date}.xlsx"),
                FileType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                Disposition("attachment")
            )

            csv_bytes = csv_str.encode("utf-8-sig")
            encoded_csv = base64.b64encode(csv_bytes).decode()
            message.attachment = Attachment(
                FileContent(encoded_csv),
                FileName(f"prospector_results_{search_date}.csv"),
                FileType("text/csv"),
                Disposition("attachment")
            )

            sg_client = sg_module.SendGridAPIClient(api_key=sg_key)
            response = sg_client.send(message)
            email_sent = response.status_code in (200, 202)
            print(f"[{datetime.now()}] Email sent via SendGrid: {response.status_code}")
        except Exception as email_err:
            email_sent = False
            print(f"[{datetime.now()}] Email error: {email_err}")

        write_status({"running": False, "stage": "Complete",
                      "job_id": job.get("job_id",""),
                      "dir_done": dir_done[0], "fin_done": fin_done[0],
                      "total": total, "started_at": start_time,
                      "completed_at": time.time(), "results_count": len(rows),
                      "ready_to_email": False, "email_sent": email_sent, "error": None})

        print(f"[{datetime.now()}] Job complete — {len(rows)} results, email_sent={email_sent}")

    except Exception as e:
        write_status({"running": False, "stage": "Error", "error": str(e),
                      "traceback": traceback.format_exc(), "ready_to_email": False})
        print(f"[{datetime.now()}] Job error: {e}")



def linkedin_company_keyword(company_name):
    """Return first 1-2 meaningful words of company name for LinkedIn search.
    Uses 2 words unless the second word is a legal suffix, in which case uses 1."""
    if not company_name:
        return ""
    SUFFIXES = {"limited","ltd","llp","plc","and co","company","group",
                "holdings","holding","services","solutions","consulting","consultancy",
                "management","associates","partnership","enterprises","ventures",
                "international","global","uk","the"}
    orig_words = company_name.split()
    if len(orig_words) >= 2 and orig_words[1].lower().rstrip(".") in SUFFIXES:
        return orig_words[0]
    return " ".join(orig_words[:min(2, len(orig_words))])

# ── Main loop ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"[{datetime.now()}] Worker started — connecting to Redis...")
    try:
        r = get_redis()
        r.ping()
        print(f"[{datetime.now()}] Redis connected OK")
    except Exception as e:
        print(f"[{datetime.now()}] Redis connection failed: {e}")

    last_job_id = None
    while True:
        try:
            r = get_redis()
            data = r.get("ch_job")
            if data:
                job = json.loads(data)
                job_id = job.get("job_id")
                if job_id and job_id != last_job_id:
                    last_job_id = job_id
                    print(f"[{datetime.now()}] New job: {job_id} — {job.get('location')} | {len(job.get('sic_codes',[]))} SIC codes")
                    run_job(job)
                    print(f"[{datetime.now()}] Job {job_id} complete")
        except Exception as e:
            print(f"[{datetime.now()}] Worker loop error: {e}")
        time.sleep(3)
