import re
import time
import os
import argparse
import requests
import pandas as pd
from bs4 import BeautifulSoup
from http.cookiejar import MozillaCookieJar

BASE_URL = "https://www.elicense.ct.gov/lookup/licenselookup.aspx"

# Our known ASP.NET control IDs (from the HTML we saw)
UPDATE_PANEL_TARGET = "ctl00$MainContentPlaceHolder$ucLicenseLookup$UpdtPanelGridLookup"
GRID_TARGET = "ctl00$MainContentPlaceHolder$ucLicenseLookup$gvSearchResults"

# Our initial “run search” argument captured from the working browser/curl request
INITIAL_SEARCH_EVENT_ARGUMENT = "11~~2~~7~~41"

# Our results table IDs (underscore version is what the HTML uses)
RESULTS_TABLE_ID = "ctl00_MainContentPlaceHolder_ucLicenseLookup_gvSearchResults"
RESULTS_TABLE_ID_ALT = "ctl00_MainContentPlaceHolder_ucLicenseLookup_gvSearchResults"  # same in this UI; kept for safety

# Our fallback schema (stable for this grid)
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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "X-MicrosoftAjax": "Delta=true",
    "X-Requested-With": "XMLHttpRequest",
    "X-Security-Request": "required",
}


def save_debug(filename: str, text: str) -> None:
    os.makedirs("outputs", exist_ok=True)
    with open(os.path.join("outputs", filename), "w", encoding="utf-8") as f:
        f.write(text)


def is_error_redirect(delta_text: str) -> bool:
    # Our delta sometimes contains: pageRedirect -> /ErrorPage.aspx?e=...
    return "pageRedirect" in delta_text and "ErrorPage.aspx" in delta_text


def load_cookies_if_provided(session: requests.Session, cookies_path: str | None) -> None:
    # Our optional cookie support for cases where the site needs a browser-established session
    if not cookies_path:
        return
    jar = MozillaCookieJar(cookies_path)
    jar.load(ignore_discard=True, ignore_expires=True)
    session.cookies.update(jar)


def parse_hidden_fields_from_html(html: str) -> dict:
    # Our initial GET provides these hidden fields used by ASP.NET postbacks
    soup = BeautifulSoup(html, "lxml")
    hidden = {}
    for field in ["__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION", "__VIEWSTATEENCRYPTED"]:
        el = soup.find(id=field)
        if el and el.has_attr("value"):
            hidden[field] = el["value"]
    return hidden


def hidden_from_delta(delta_text: str, field_name: str) -> str | None:
    # Our delta format includes: |hiddenField|__VIEWSTATE|VALUE|
    m = re.search(rf"\|hiddenField\|{re.escape(field_name)}\|([\s\S]*?)\|", delta_text)
    return m.group(1) if m else None


def update_hidden_from_delta(hidden: dict, delta_text: str) -> None:
    # Our VIEWSTATE / EVENTVALIDATION may update after each postback
    for key in ["__VIEWSTATE", "__EVENTVALIDATION", "__VIEWSTATEENCRYPTED"]:
        val = hidden_from_delta(delta_text, key)
        if val:
            hidden[key] = val


def initial_get(session: requests.Session) -> dict:
    # Our initial page load sets cookies and gives us hidden fields
    r = session.get(BASE_URL, headers={"User-Agent": HEADERS["User-Agent"], "Accept": "text/html"})
    r.raise_for_status()

    hidden = parse_hidden_fields_from_html(r.text)
    if "__VIEWSTATE" not in hidden or "__VIEWSTATEGENERATOR" not in hidden:
        save_debug("debug_initial_get.html", r.text)
        raise RuntimeError("Missing VIEWSTATE fields on initial GET. Saved outputs/debug_initial_get.html")

    return hidden


def extract_max_page(delta_text: str) -> int:
    # Our pager links look like: Page$2, Page$3, ...
    pages = [int(x) for x in re.findall(r"Page\$(\d+)", delta_text)]
    return max(pages) if pages else 1


def extract_table_df(delta_text: str) -> pd.DataFrame:
    # Our delta format contains updatePanel blocks like:
    # |updatePanel|SOME_ID|<div>...html...</div>|
    update_panels = re.findall(r"\|updatePanel\|[^|]+\|([\s\S]*?)\|", delta_text)
    if not update_panels:
        save_debug("debug_no_update_panels.txt", delta_text)
        raise ValueError("No updatePanel blocks found. Saved outputs/debug_no_update_panels.txt")

    for panel_html in update_panels:
        soup = BeautifulSoup(panel_html, "lxml")
        table = soup.find("table", id=RESULTS_TABLE_ID) or soup.find("table", id=RESULTS_TABLE_ID_ALT)
        if table is None:
            continue

        headers = None
        header_row = table.find("tr", class_="CavuGridHeader")
        if header_row and header_row.find_all("th"):
            headers = [th.get_text(strip=True).replace("\xa0", "") for th in header_row.find_all("th")]
            if headers and headers[0] == "":
                headers[0] = "Detail"
        else:
            headers = FALLBACK_HEADERS

        rows = []
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if not tds:
                continue
            row = [td.get_text(" ", strip=True).replace("\xa0", "") for td in tds]
            if len(row) == len(headers):
                rows.append(row)

        df = pd.DataFrame(rows, columns=headers)

        if df.empty:
            save_debug("debug_empty_delta.txt", delta_text)
            save_debug("debug_empty_table.html", str(table))

        return df

    save_debug("debug_no_results_table.txt", delta_text)
    raise ValueError("No results table found in any updatePanel. Saved outputs/debug_no_results_table.txt")


def ajax_postback(session: requests.Session, hidden: dict, event_target: str, event_argument: str = "") -> str:
    # Our ASP.NET AJAX postback requires ScriptManager wiring to the update panel
    script_manager = f"{UPDATE_PANEL_TARGET}|{event_target}"

    payload = {
        "__VIEWSTATE": hidden.get("__VIEWSTATE", ""),
        "__VIEWSTATEGENERATOR": hidden.get("__VIEWSTATEGENERATOR", ""),
        "__EVENTVALIDATION": hidden.get("__EVENTVALIDATION", ""),
        "__VIEWSTATEENCRYPTED": hidden.get("__VIEWSTATEENCRYPTED", ""),

        "__EVENTTARGET": event_target,
        "__EVENTARGUMENT": event_argument,
        "__ASYNCPOST": "true",
        "ctl00$ScriptManager1": script_manager,

        # Our filters: Dentist + CT + Active
        "ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$lbMultipleCredentialTypePrefix": "137",
        "ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$ddStates": "CT",
        "ctl00$MainContentPlaceHolder$ucLicenseLookup$ctl03$ddStatus": "368",
    }

    r = session.post(BASE_URL, headers=HEADERS, data=payload, allow_redirects=False)
    r.raise_for_status()
    return r.text or ""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cookies", default=None, help="Optional cookies.txt in Netscape format")
    args = parser.parse_args()

    os.makedirs("outputs", exist_ok=True)

    session = requests.Session()
    load_cookies_if_provided(session, args.cookies)

    hidden = initial_get(session)

    # Our page-1 call must use the update panel event + the working event argument
    delta = ajax_postback(
        session,
        hidden,
        event_target=UPDATE_PANEL_TARGET,
        event_argument=INITIAL_SEARCH_EVENT_ARGUMENT,
    )

    if is_error_redirect(delta):
        save_debug("debug_error_redirect.txt", delta)
        raise RuntimeError(
            "Redirected to ErrorPage.aspx. Saved outputs/debug_error_redirect.txt\n"
            "Our next step: run with browser cookies if the site requires a verified session:\n"
            "  python .\\src\\connecticut.py --cookies cookies.txt"
        )

    update_hidden_from_delta(hidden, delta)

    df_all = []
    df_page1 = extract_table_df(delta)

    if df_page1.empty:
        print("WARNING: Page 1 returned 0 rows.")
        print("Check outputs/debug_empty_delta.txt and outputs/debug_empty_table.html")
        df_page1.to_csv(OUTPUT_CSV, index=False)
        return

    df_all.append(df_page1)

    max_page = extract_max_page(delta)
    print(f"Detected {max_page} pages")
    print(f"Fetched page 1/{max_page} rows={len(df_page1)}")

    # Our page-2..N calls use the grid paging postback
    for p in range(2, max_page + 1):
        time.sleep(0.8)

        delta = ajax_postback(session, hidden, event_target=GRID_TARGET, event_argument=f"Page${p}")
        if is_error_redirect(delta):
            save_debug(f"debug_error_redirect_page_{p}.txt", delta)
            raise RuntimeError(f"Redirected to ErrorPage on page {p}. Saved outputs/debug_error_redirect_page_{p}.txt")

        update_hidden_from_delta(hidden, delta)

        df_p = extract_table_df(delta)
        print(f"Fetched page {p}/{max_page} rows={len(df_p)}")
        df_all.append(df_p)

    df = pd.concat(df_all, ignore_index=True).drop_duplicates()
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"\nSUCCESS: Saved {len(df)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
