import requests
from bs4 import BeautifulSoup
import math
import os
import csv
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

def get_verification_token():
    """Extract CSRF token from the Removed page."""
    print("Fetching page to extract CSRF token...")
    r = session.get(PAGE_URL)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    token_input = soup.find("input", {"name": "__RequestVerificationToken"})
    if not token_input:
        raise ValueError("Could not find CSRF token on Removed page!")
    return token_input["value"]

def fetch_page(start, length, token):
    """Fetch one page via the RemovedPublic DataTables API."""
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
        "X-Requested-With": "XMLHttpRequest"
    }

    r = session.post(API_URL, data=payload, headers=headers)
    r.raise_for_status()
    return r.json()

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

    # Fetch first page to determine the total
    print("Fetching first Removed page...")
    first = fetch_page(start=0, length=10, token=token)
    total_records = first["recordsTotal"]
    print(f"Total removed records: {total_records}")

    page_size = 100
    total_pages = math.ceil(total_records / page_size)

    all_rows = first["data"]

    for i in tqdm(range(1, total_pages), desc="Fetching removed pages"):
        start = i * page_size
        page_json = fetch_page(start=start, length=page_size, token=token)
        all_rows.extend(page_json["data"])

    print(f"Downloaded {len(all_rows)} removed records.")

    # Save weekly snapshot
    save_to_csv(all_rows, snapshot_csv)

    # Append to master file
    master_rows = []
    if os.path.exists(master_csv):
        with open(master_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            master_rows = list(reader)

    # Only append new ones
    existing_ids = {r.get("ProviderNumber") for r in master_rows if r.get("ProviderNumber")}
    for row in all_rows:
        # choose a key column appropriate for this dataset
        if row.get("ProviderNumber") not in existing_ids:
            master_rows.append(row)

    save_to_csv(master_rows, master_csv)

if __name__ == "__main__":
    main()
