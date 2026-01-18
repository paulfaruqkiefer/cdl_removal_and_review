import requests
from bs4 import BeautifulSoup
import csv
import os
import time
from datetime import date
from tqdm import tqdm

# ======================
# Config
# ======================
BASE_URL = "https://tpr.fmcsa.dot.gov"
REMOVED_URL = f"{BASE_URL}/Provider/Removed"

OUTPUT_DIR = "outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

TODAY = date.today().strftime("%Y-%m-%d")
SNAPSHOT_CSV = os.path.join(OUTPUT_DIR, f"fmcsa_removed_{TODAY}.csv")
MASTER_CSV = os.path.join(OUTPUT_DIR, "master_fmcsa_removed.csv")

PAGE_SLEEP = 0.5  # polite pacing

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/html",
})

# ======================
# Helpers
# ======================

def parse_removed_table(soup):
    rows = []
    table = soup.find("table", {"id": "DataTable"})
    if not table:
        raise RuntimeError("Could not find Removed Providers table on page")
    
    tbody = table.find("tbody")
    for tr in tbody.find_all("tr"):
        cols = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(cols) < 5:
            continue
        row = {
            "ProviderName": cols[0],
            "City": cols[1],
            "State": cols[2],
            "Date": cols[3],
            "Type": cols[4],  # TypeOfRemoval
            "CompositeKey": f"{cols[0]}|{cols[1]}|{cols[2]}|{cols[3]}"
        }
        rows.append(row)
    return rows

def save_csv(rows, path):
    if not rows:
        print(f"⚠️ No rows to save: {path}")
        return
    fieldnames = sorted({k for row in rows for k in row.keys()})
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"💾 Saved: {path}")

# ======================
# Main
# ======================

def main():
    print("📥 Fetching Removed Providers page...")
    all_rows = []
    page_number = 1
    next_url = REMOVED_URL

    while next_url:
        r = session.get(next_url, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        page_rows = parse_removed_table(soup)
        all_rows.extend(page_rows)
        print(f"Page {page_number}: {len(page_rows)} rows")

        # DataTables "Next" button handling
        next_btn = soup.find("a", {"id": "DataTable_next"})
        if next_btn and "disabled" not in next_btn.get("class", []):
            href = next_btn.get("href")
            if href and href != "#":
                next_url = BASE_URL + href
            else:
                next_url = None
        else:
            next_url = None

        page_number += 1
        time.sleep(PAGE_SLEEP)

    print(f"✅ Total removed providers scraped: {len(all_rows)}")

    # --- Save weekly snapshot ---
    save_csv(all_rows, SNAPSHOT_CSV)

    # --- Update master CSV ---
    master_rows = []
    if os.path.exists(MASTER_CSV):
        with open(MASTER_CSV, "r", encoding="utf-8") as f:
            master_rows = list(csv.DictReader(f))

    existing_keys = {r.get("CompositeKey") for r in master_rows if r.get("CompositeKey")}
    new_rows = [r for r in all_rows if r.get("CompositeKey") not in existing_keys]

    if new_rows:
        master_rows.extend(new_rows)
        save_csv(master_rows, MASTER_CSV)
        print(f"➕ Added {len(new_rows)} new providers to master file")
    else:
        print("ℹ️ No new providers to add to master file")

if __name__ == "__main__":
    main()
