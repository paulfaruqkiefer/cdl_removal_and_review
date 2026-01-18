import requests
from bs4 import BeautifulSoup
import math
import os
import csv
import time
from datetime import date
from tqdm import tqdm

# --- URLs for Removed Providers ---
BASE_URL = "https://tpr.fmcsa.dot.gov"
PAGE_URL = BASE_URL + "/Provider/Removed"
API_URL = BASE_URL + "/api/Public/RemovedPublic"

# --- Files & Directories ---
os.makedirs("outputs", exist_ok=True)
today = date.today().strftime("%Y-%m-%d")
snapshot_csv = os.path.join("outputs", f"fmcsa_removed_{today}.csv")
master_csv = os.path.join("outputs", "master_fmcsa_removed.csv")

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
})


def get_verification_token():
    """Extract CSRF token from the Removed page."""
    print("Fetching page to extract CSRF token...")
    r = session.get(PAGE_URL, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    token_input = soup.find("input", {"name": "__RequestVerificationToken"})
    if not token_input:
        raise RuntimeError("Could not find CSRF token on Removed page")

    return token_input["value"]


def fetch_page(start, length, token, retries=5, sleep=3):
    """Fetch one page via the RemovedPublic DataTables API (with retries)."""

    payload = {
        "draw": 1,
        "start": start,
        "length": length,
        "order[0][column]": 2,
        "order[0][dir]": "desc",
        "columns[0][data]": "ProviderName",
        "columns[1][data]": "City",
        "columns[2][data]": "State",
        "columns[3][data]": "DateOfRemoval",
        "columns[4][data]": "TypeOfRemoval",
        "__RequestVerificationToken": token,
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": PAGE_URL
    }

    for attempt in range(1, retries + 1):
        try:
            r = session.post(
                API_URL,
                data=payload,
                headers=headers,
                timeout=30
            )

            if r.status_code == 500:
                print(f"⚠️ 500 error (attempt {attempt}/{retries}) — retrying...")
                time.sleep(sleep)
                continue

            r.raise_for_status()
            return r.json()

        except requests.exceptions.RequestException as e:
            print(f"⚠️ Request failed (attempt {attempt}/{retries}): {e}")
            time.sleep(sleep)

    raise RuntimeError("FMCSA Removed API failed after multiple retries")


def save_to_csv(rows, output_file):
    """Write rows to a CSV file."""
    all_keys = set()
    for row in rows:
        all_keys.update(row.keys())

    fieldnames = sorted(all_keys)

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved CSV: {output_file}")


def main():
    token = get_verification_token()

    print("Fetching first Removed page...")
    first = fetch_page(start=0, length=10, token=token)
    total_records = first.get("recordsTotal", 0)

    print(f"Total removed records: {total_records}")

    page_size = 100
    total_pages = math.ceil(total_records / page_size)

    all_rows = first["data"]

    for i in tqdm(range(1, total_pages), desc="Fetching removed pages"):
        start = i * page_size
        page_json = fetch_page(start=start, length=page_size, token=token)
        all_rows.extend(page_json["data"])
        time.sleep(0.5)  # polite pacing

    print(f"Downloaded {len(all_rows)} removed records.")

    # --- Save weekly snapshot ---
    save_to_csv(all_rows, snapshot_csv)

    # --- Update master file ---
    master_rows = []
    if os.path.exists(master_csv):
        with open(master_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            master_rows = list(reader)

    existing_ids = {
        r.get("ProviderNumber")
        for r in master_rows
        if r.get("ProviderNumber")
    }

    new_count = 0
    for row in all_rows:
        pid = row.get("ProviderNumber")
        if pid and pid not in existing_ids:
            master_rows.append(row)
            new_count += 1

    save_to_csv(master_rows, master_csv)
    print(f"Added {new_count} new providers to master file.")


if __name__ == "__main__":
    main()
