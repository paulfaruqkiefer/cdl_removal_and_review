import requests
from bs4 import BeautifulSoup
import csv
import math
import os
from datetime import date

# URLs
BASE_URL = "https://tpr.fmcsa.dot.gov"
PAGE_URL = BASE_URL + "/Provider/InReview"
API_URL = BASE_URL + "/api/Public/InReviewPublic"

# Ensure outputs folder exists
os.makedirs("outputs", exist_ok=True)

# Files
master_csv = os.path.join("outputs", "master_fmcsa.csv")
snapshot_csv = os.path.join("outputs", f"fmcsa_in_review_{date.today()}.csv")

# Session for requests
session = requests.Session()

def get_verification_token():
    """Fetch the HTML page and extract CSRF __RequestVerificationToken."""
    print("Fetching page to extract CSRF token...")
    r = session.get(PAGE_URL)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    token_input = soup.find("input", {"name": "__RequestVerificationToken"})
    if not token_input:
        raise ValueError("Could not find __RequestVerificationToken in page HTML!")
    return token_input["value"]

def fetch_page(start, length, token):
    """Fetch one page of results using the DataTables API."""
    payload = {
        "draw": 1,
        "start": start,
        "length": length,
        "order[0][column]": 2,
        "order[0][orderable]": "true",
        "order[0][dir]": "desc",
        "columns[0][data]": "Name",
        "columns[1][data]": "City",
        "columns[2][data]": "PhysicalState",
        "search[regex]": "false",
        "__RequestVerificationToken": token,
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
    }

    r = session.post(API_URL, data=payload, headers=headers)
    r.raise_for_status()
    return r.json()

def save_csv(rows, output_file):
    """Save rows to a CSV file."""
    # Union of all keys
    all_keys = set()
    for row in rows:
        all_keys.update(row.keys())
    fieldnames = sorted(all_keys)

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved CSV to: {output_file}")

def main():
    token = get_verification_token()

    print("Fetching first page to determine total rows...")
    page_size = 10
    first = fetch_page(start=0, length=page_size, token=token)
    total_records = first["recordsTotal"]
    print(f"Total records: {total_records}")

    total_pages = math.ceil(total_records / page_size)
    all_rows = first["data"]

    # Fetch remaining pages
    for page in range(1, total_pages):
        print(f"Fetching page {page+1}/{total_pages}...")
        start = page * page_size
        page_json = fetch_page(start=start, length=page_size, token=token)
        all_rows.extend(page_json["data"])

    print(f"Total rows downloaded: {len(all_rows)}")

    # Save weekly snapshot
    save_csv(all_rows, snapshot_csv)

    # Update master CSV
    master_rows = []
    if os.path.exists(master_csv):
        with open(master_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            master_rows = list(reader)

    # Add new rows to master if not already present
    existing_ids = {row.get("USDOTNumber") for row in master_rows if "USDOTNumber" in row}
    for row in all_rows:
        if row.get("USDOTNumber") not in existing_ids:
            master_rows.append(row)

    save_csv(master_rows, master_csv)

if __name__ == "__main__":
    main()
