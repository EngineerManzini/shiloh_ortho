import re
import os
import time
import argparse
import requests
import concurrent.futures
import pandas as pd
from bs4 import BeautifulSoup
from http.cookiejar import MozillaCookieJar
from urllib.parse import quote

BASE_URL = "https://www.elicense.ct.gov/lookup/licenselookup.aspx"
DETAIL_BASE_URL = "https://www.elicense.ct.gov/Lookup/licensedetail.aspx"

UPDATE_PANEL_TARGET = "ctl00$MainContentPlaceHolder$ucLicenseLookup$UpdtPanelGridLookup"
GRID_TARGET = "ctl00$MainContentPlaceHolder$ucLicenseLookup$gvSearchResults"

INITIAL_SEARCH_EVENT_ARGUMENT = "11~~2~~7~~41"

RESULTS_TABLE_ID = "ctl00_MainContentPlaceHolder_ucLicenseLookup_gvSearchResults"

FALLBACK_HEADERS = [
    "Detail",
    "Name",
    "Credential",
    "Credential Description",
    "Status",
    "Status Reason",
    "City",
    "DBA",
]

OUTPUT_CSV = "outputs/connecticut_dentists_landing.csv"

# -----------------------------
# *** Detail field mapping ***
# Maps normalized column header text → output CSV column name.
# The detail page uses horizontal tables (thead headers / tbody single row).
# Add entries here if new tables appear on the detail page.
# -----------------------------
DETAIL_FIELD_MAP = {
    # Grid0 — Name table
    "name":                             "detail_name",

    # Grid1 — License Information table
    "license_type":                     "detail_license_type",
    "license_number":                   "detail_license_number",
    "expiration_date":                  "detail_expiration_date",
    "granted_date":                     "detail_granted_date",
    "license_name":                     "detail_license_name",
    "license_status":                   "detail_license_status",
    # the blank " " column maps to a generic status qualifier
    "":                                 "detail_status_qualifier",
    "licensure_actions_or_pending_charges": "detail_licensure_actions",
}

# All unique output column names (preserving insertion order)
DETAIL_COLUMNS = list(dict.fromkeys(DETAIL_FIELD_MAP.values()))


HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Connection": "keep-alive",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://www.elicense.ct.gov",
    "Referer": BASE_URL,
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "X-MicrosoftAjax": "Delta=true",
    "X-Requested-With": "XMLHttpRequest",
    "X-Security-Request": "required",
}

DETAIL_HEADERS = {
    "Accept": "text/html, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Referer": BASE_URL,
    "User-Agent": HEADERS["User-Agent"],
    "X-Requested-With": "XMLHttpRequest",
}


# -----------------------------
# Utilities
# -----------------------------

def ensure_output():
    os.makedirs("outputs", exist_ok=True)


def save_debug(name, text):
    ensure_output()
    with open(os.path.join("outputs", name), "w", encoding="utf-8") as f:
        f.write(text)


# -----------------------------
# Session setup
# -----------------------------

def load_cookies(session, cookies_path):
    if not cookies_path:
        return
    jar = MozillaCookieJar(cookies_path)
    jar.load(ignore_discard=True, ignore_expires=True)
    session.cookies.update(jar)


def parse_hidden_fields_from_html(html):
    soup = BeautifulSoup(html, "lxml")
    hidden = {}

    for field in [
        "__VIEWSTATE",
        "__VIEWSTATEGENERATOR",
        "__EVENTVALIDATION",
        "__VIEWSTATEENCRYPTED",
    ]:
        el = soup.find(id=field)
        if el and el.has_attr("value"):
            hidden[field] = el.get("value", "")

    return hidden


def initial_get(session):
    r = session.get(BASE_URL, headers={"User-Agent": HEADERS["User-Agent"], "Accept": "text/html"})
    r.raise_for_status()

    hidden = parse_hidden_fields_from_html(r.text)

    if "__VIEWSTATE" not in hidden or "__VIEWSTATEGENERATOR" not in hidden:
        save_debug("debug_initial_get.html", r.text)
        raise RuntimeError("Missing VIEWSTATE fields on initial GET")

    return hidden


# -----------------------------
# Delta helpers
# -----------------------------

def extract_hidden(delta_text, field_name):
    m = re.search(rf"\|hiddenField\|{re.escape(field_name)}\|([\s\S]*?)\|", delta_text)
    return m.group(1) if m else None


def update_hidden(hidden, delta_text):
    for key in ["__VIEWSTATE", "__EVENTVALIDATION", "__VIEWSTATEENCRYPTED"]:
        val = extract_hidden(delta_text, key)
        if val:
            hidden[key] = val


def is_error_redirect(delta_text):
    return "pageRedirect" in delta_text and "ErrorPage.aspx" in delta_text


# -----------------------------
# AJAX search / pagination
# -----------------------------

def ajax_postback(session, hidden, event_target, event_argument=""):
    payload = {
        "__VIEWSTATE": hidden.get("__VIEWSTATE", ""),
        "__VIEWSTATEGENERATOR": hidden.get("__VIEWSTATEGENERATOR", ""),
        "__EVENTVALIDATION": hidden.get("__EVENTVALIDATION", ""),
        "__VIEWSTATEENCRYPTED": hidden.get("__VIEWSTATEENCRYPTED", ""),
        "__EVENTTARGET": event_target,
        "__EVENTARGUMENT": event_argument,
        "__ASYNCPOST": "true",
        "ctl00$ScriptManager1": f"{UPDATE_PANEL_TARGET}|{event_target}",

        "ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$lbMultipleCredentialTypePrefix": "137",
        "ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$ddCredPrefix": "",
        "ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$tbLicenseNumber": "",
        "ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$tbLicenseSuffix": "",
        "ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$ddSubCategory": "",
        "ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$ddStatus": "368",
        "ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$tbDBA_Contact": "",
        "ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$tbFirstName_Contact": "",
        "ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$tbLastName_Contact": "",
        "ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$tbAddress2_ContactAddress": "",
        "ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$ddStates": "CT",
        "ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$tbCity_ContactAddress": "",
        "ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$tbZipCode_ContactAddress": "",
        "ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$ddCountry": "221",
        "ctl00$MainContentPlaceHolder$ucLicenseLookup$ResizeLicDetailPopupID_ClientState": "0,0",
        "ctl00$OutsidePlaceHolder$ucLicenseDetailPopup$ResizeLicDetailPopupID_ClientState": "0,0",
    }

    r = session.post(BASE_URL, headers=HEADERS, data=payload, allow_redirects=False)
    r.raise_for_status()
    return r.text or ""


def detect_pages(delta_text):
    pages = [int(x) for x in re.findall(r"Page\$(\d+)", delta_text)]
    return max(pages) if pages else 1


# -----------------------------
# Landing table parsing
# -----------------------------

def extract_table_rows(delta_text):
    update_panels = re.findall(r"\|updatePanel\|[^|]+\|([\s\S]*?)\|", delta_text)
    if not update_panels:
        save_debug("debug_no_update_panels.txt", delta_text)
        raise RuntimeError("No updatePanel blocks found")

    for panel_html in update_panels:
        soup = BeautifulSoup(panel_html, "lxml")
        table = soup.find("table", id=RESULTS_TABLE_ID)

        if table is None:
            continue

        header_row = table.find("tr", class_="CavuGridHeader")
        if header_row and header_row.find_all("th"):
            headers = [
                th.get_text(strip=True).replace("\xa0", "")
                for th in header_row.find_all("th")
            ]
            if headers and headers[0] == "":
                headers[0] = "Detail"
        else:
            headers = FALLBACK_HEADERS[:]

        tbody = table.find("tbody") or table
        rows = []

        for tr in tbody.find_all("tr"):
            tds = tr.find_all("td", recursive=False)
            if not tds:
                continue

            row_values = []
            detail_id = ""

            for idx, td in enumerate(tds):
                text_value = td.get_text(" ", strip=True).replace("\xa0", "")

                if idx == 0:
                    link = td.find("a", href=True)
                    if link:
                        href = link.get("href", "")
                        m = re.search(r"DisplayLicenceDetail\('([^']+)'\)", href)
                        if m:
                            detail_id = m.group(1)
                    row_values.append(text_value)
                else:
                    row_values.append(text_value)

            if len(row_values) > len(headers):
                row_values = row_values[:len(headers)]
            if len(row_values) < len(headers):
                row_values += [""] * (len(headers) - len(row_values))

            if all(cell.strip() == "" for cell in row_values):
                continue

            record = dict(zip(headers, row_values))
            record["detail_id"] = detail_id
            rows.append(record)

        if not rows:
            save_debug("debug_empty_table.html", str(table))
            return pd.DataFrame(columns=headers + ["detail_id"])

        df = pd.DataFrame(rows)

        if "Name" in df.columns:
            df = df[df["Name"].fillna("").astype(str).str.strip() != ""]

        return df.reset_index(drop=True)

    save_debug("debug_no_results_table.txt", delta_text)
    raise RuntimeError("Results table not found in any updatePanel")


# -----------------------------
# Detail page fetch + parsing
# -----------------------------

def fetch_detail_html(session, detail_id, debug=False):
    if not detail_id:
        if debug:
            print(f"  [DEBUG] fetch_detail_html called with empty detail_id — skipping")
        return ""

    params = {
        "id": detail_id,
        "_": str(int(time.time() * 1000)),
    }

    r = session.get(DETAIL_BASE_URL, headers=DETAIL_HEADERS, params=params)
    r.raise_for_status()

    if debug:
        print(f"  [DEBUG] detail_id={detail_id!r}  status={r.status_code}  len={len(r.text)}")
        save_debug(f"debug_detail_sample_{detail_id}.html", r.text)

    return r.text


def normalize_label(text):
    return re.sub(r"[^a-z0-9]+", "_", text.strip().lower()).strip("_")


def parse_detail_html(html):
    """
    Parse a detail page where data is stored in horizontal Bootstrap tables:
      - <thead> contains <th> column headers
      - <tbody> contains a single <tr> of values

    Each table's headers are mapped through DETAIL_FIELD_MAP to fixed
    output column names.  Every DETAIL_COLUMN is always present ('' if absent).
    """
    result = {col: "" for col in DETAIL_COLUMNS}

    if not html:
        return result

    soup = BeautifulSoup(html, "lxml")

    for table in soup.find_all("table"):
        thead = table.find("thead")
        tbody = table.find("tbody")
        if not thead or not tbody:
            continue

        headers = [
            normalize_label(th.get_text(" ", strip=True).replace("\xa0", ""))
            for th in thead.find_all("th")
        ]

        # Only use the first data row
        data_row = tbody.find("tr")
        if not data_row:
            continue

        values = [
            td.get_text(" ", strip=True).replace("\xa0", "")
            for td in data_row.find_all("td")
        ]

        for header, value in zip(headers, values):
            col = DETAIL_FIELD_MAP.get(header)
            if col and not result[col]:   # first match wins
                result[col] = value

    return result


def _process_detail(session, record, debug=False):
    detail_id = record.get("detail_id", "")
    if detail_id:
        detail_html = fetch_detail_html(session, detail_id, debug=debug)
        if detail_html:
            detail_data = parse_detail_html(detail_html)
            if debug:
                filled = {k: v for k, v in detail_data.items() if v}
                print(f"  [DEBUG] parsed fields for {detail_id}: {list(filled.keys()) or 'NONE'}")
            record.update(detail_data)
        else:
            if debug:
                print(f"  [DEBUG] empty HTML returned for detail_id={detail_id!r}")
            record.update({col: "" for col in DETAIL_COLUMNS})
    else:
        if debug:
            print(f"  [DEBUG] no detail_id on record: {record.get('Name', '?')}")
        record.update({col: "" for col in DETAIL_COLUMNS})
    return record


def enrich_with_details(session, landing_df, max_workers=10):
    if landing_df.empty:
        return landing_df

    records = landing_df.to_dict("records")
    enriched_rows = [None] * len(records)

    print(f"Enriching {len(records)} detail pages using {max_workers} threads...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_idx = {
            executor.submit(_process_detail, session, record.copy()): idx
            for idx, record in enumerate(records)
        }

        completed = 0
        for future in concurrent.futures.as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                enriched_rows[idx] = future.result()
            except Exception as e:
                print(f"Error enriching row {idx}: {e}")
                row = records[idx].copy()
                row.update({col: "" for col in DETAIL_COLUMNS})
                enriched_rows[idx] = row

            completed += 1
            if completed % 25 == 0:
                print(f"Enriched {completed}/{len(records)} detail pages")

    df = pd.DataFrame(enriched_rows)

    # Guarantee every detail column exists and is in a predictable order
    for col in DETAIL_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    # Reorder: landing columns first, then detail columns
    landing_cols = [c for c in df.columns if c not in DETAIL_COLUMNS]
    df = df[landing_cols + DETAIL_COLUMNS]

    return df


# -----------------------------
# Main pipeline
# -----------------------------

def run_pipeline(session, hidden):
    results = []

    delta = ajax_postback(
        session,
        hidden,
        event_target=UPDATE_PANEL_TARGET,
        event_argument=INITIAL_SEARCH_EVENT_ARGUMENT,
    )

    if is_error_redirect(delta):
        save_debug("debug_error_redirect.txt", delta)
        raise RuntimeError("Redirected to ErrorPage.aspx on initial search")

    update_hidden(hidden, delta)

    df_page1 = extract_table_rows(delta)
    results.append(df_page1)

    max_page = detect_pages(delta)
    print(f"Detected {max_page} pages")
    print(f"Fetched page 1/{max_page} rows={len(df_page1)}")

    for p in range(2, max_page + 1):
        time.sleep(0.8)

        delta = ajax_postback(
            session,
            hidden,
            event_target=GRID_TARGET,
            event_argument=f"Page${p}",
        )

        if is_error_redirect(delta):
            save_debug(f"debug_error_redirect_page_{p}.txt", delta)
            raise RuntimeError(f"Redirected to ErrorPage on page {p}")

        update_hidden(hidden, delta)

        df_page = extract_table_rows(delta)
        print(f"Fetched page {p}/{max_page} rows={len(df_page)}")
        results.append(df_page)

    landing_df = pd.concat(results, ignore_index=True).drop_duplicates()
    return landing_df


# -----------------------------
# Entry point
# -----------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cookies", default=None, help="Optional cookies.txt in Netscape format")
    parser.add_argument(
        "--skip-details",
        action="store_true",
        help="Only save landing grid rows without fetching per-dentist detail pages",
    )
    parser.add_argument(
        "--debug-details",
        action="store_true",
        help="Print detail fetch diagnostics for first 3 rows and save sample HTML to outputs/",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=10,
        help="Number of concurrent threads for fetching detail pages",
    )
    args = parser.parse_args()

    ensure_output()

    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=args.workers, pool_maxsize=args.workers)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    load_cookies(session, args.cookies)

    hidden = initial_get(session)

    landing_df = run_pipeline(session, hidden)

    if landing_df.empty:
        print("WARNING: No landing rows found")
        landing_df.to_csv(OUTPUT_CSV, index=False)
        return

    if args.skip_details:
        landing_df.to_csv(OUTPUT_CSV, index=False)
        print(f"SUCCESS: Saved {len(landing_df)} landing rows to {OUTPUT_CSV}")
        return

    print("Fetching individual dentist detail pages...")

    if args.debug_details:
        print("\n--- DEBUG: testing first 3 detail pages ---")
        for _, row in landing_df.head(1).iterrows():   # just 1 is enough
            record = row.to_dict()
            detail_id = record.get("detail_id", "")
            html = fetch_detail_html(session, detail_id, debug=True)
            if html:
                soup = BeautifulSoup(html, "lxml")
                print("\n[STRUCTURE] All <tr> blocks found:")
                for i, tr in enumerate(soup.find_all("tr")[:30]):
                    cells = tr.find_all(["td", "th"], recursive=False)
                    texts = [c.get_text(" ", strip=True).replace("\xa0", " ")[:60] for c in cells]
                    print(f"  tr[{i}] ({len(cells)} cells): {texts}")
                print("\n[STRUCTURE] All <div> blocks with text:")
                for div in soup.find_all("div"):
                    t = div.get_text(" ", strip=True).replace("\xa0", " ")
                    if 5 < len(t) < 120 and not div.find("div"):  # leaf divs only
                        print(f"  div.{div.get('class', ['?'])[0]}: {t[:100]}")
        print("\n--- Check outputs/ for debug_detail_sample_*.html ---\n")
        return   # stop here so you can review before full run

    final_df = enrich_with_details(session, landing_df, max_workers=args.workers)

    try:
        final_df.to_csv(OUTPUT_CSV, index=False)
        print(f"SUCCESS: Saved {len(final_df)} enriched rows to {OUTPUT_CSV}")
    except PermissionError:
        alt = OUTPUT_CSV.replace(".csv", "_new.csv")
        final_df.to_csv(alt, index=False)
        print(f"WARNING: '{OUTPUT_CSV}' is open (Excel?). Saved to '{alt}' instead.")


if __name__ == "__main__":
    main()
    
