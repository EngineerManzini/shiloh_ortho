import re
import os
import time
import argparse
import requests
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

        # Our search filters
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

                # Our first column contains the Detail button and hidden detail token
                if idx == 0:
                    link = td.find("a", href=True)
                    if link:
                        href = link.get("href", "")
                        m = re.search(r"DisplayLicenceDetail\('([^']+)'\)", href)
                        if m:
                            detail_id = m.group(1)
                    # We keep the visible button text in the landing file
                    row_values.append(text_value)
                else:
                    row_values.append(text_value)

            # Normalize row width
            if len(row_values) > len(headers):
                row_values = row_values[:len(headers)]
            if len(row_values) < len(headers):
                row_values += [""] * (len(headers) - len(row_values))

            # Skip blank rows
            if all(cell.strip() == "" for cell in row_values):
                continue

            record = dict(zip(headers, row_values))
            record["detail_id"] = detail_id
            rows.append(record)

        if not rows:
            save_debug("debug_empty_table.html", str(table))
            return pd.DataFrame(columns=headers + ["detail_id"])

        df = pd.DataFrame(rows)

        # Drop pager rows that sometimes slip through
        if "Name" in df.columns:
            df = df[df["Name"].fillna("").astype(str).str.strip() != ""]

        return df.reset_index(drop=True)

    save_debug("debug_no_results_table.txt", delta_text)
    raise RuntimeError("Results table not found in any updatePanel")


# -----------------------------
# Detail page fetch + parsing
# -----------------------------

def fetch_detail_html(session, detail_id):
    if not detail_id:
        return ""

    # We keep the timestamp parameter because the browser includes it
    params = {
        "id": detail_id,
        "_": str(int(time.time() * 1000)),
    }

    r = session.get(DETAIL_BASE_URL, headers=DETAIL_HEADERS, params=params)
    r.raise_for_status()
    return r.text


def normalize_label(text):
    return re.sub(r"[^a-z0-9]+", "_", text.strip().lower()).strip("_")


def parse_detail_html(html):
    if not html:
        return {}

    soup = BeautifulSoup(html, "lxml")
    detail_data = {}

    # Our most common pattern: table rows with label/value cells
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["td", "th"], recursive=False)
        if len(cells) < 2:
            continue

        label = cells[0].get_text(" ", strip=True).replace("\xa0", "")
        value = cells[1].get_text(" ", strip=True).replace("\xa0", "")

        if not label:
            continue

        key = normalize_label(label)

        if key and value and key not in detail_data:
            detail_data[key] = value

    # Our fallback: some layouts use adjacent label/value divs or spans
    if not detail_data:
        text_lines = [line.strip() for line in soup.get_text("\n").splitlines() if line.strip()]
        for i in range(len(text_lines) - 1):
            label = text_lines[i]
            value = text_lines[i + 1]
            if len(label) <= 60 and ":" in label:
                key = normalize_label(label.replace(":", ""))
                if key and key not in detail_data:
                    detail_data[key] = value

    return detail_data


def enrich_with_details(session, landing_df, sleep_seconds=0.3):
    if landing_df.empty:
        return landing_df

    enriched_rows = []

    for idx, row in landing_df.iterrows():
        record = row.to_dict()
        detail_id = record.get("detail_id", "")

        detail_html = fetch_detail_html(session, detail_id)
        if detail_html:
            detail_data = parse_detail_html(detail_html)
            record.update(detail_data)

        enriched_rows.append(record)

        # Our polite delay keeps the requests from hammering the site
        time.sleep(sleep_seconds)

        if (idx + 1) % 25 == 0:
            print(f"Enriched {idx + 1}/{len(landing_df)} detail pages")

    return pd.DataFrame(enriched_rows)


# -----------------------------
# Main pipeline
# -----------------------------

def run_pipeline(session, hidden):
    results = []

    # Our page-1 call uses the working update panel trigger
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
    args = parser.parse_args()

    ensure_output()

    session = requests.Session()
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
    final_df = enrich_with_details(session, landing_df)

    final_df.to_csv(OUTPUT_CSV, index=False)
    print(f"SUCCESS: Saved {len(final_df)} enriched rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
