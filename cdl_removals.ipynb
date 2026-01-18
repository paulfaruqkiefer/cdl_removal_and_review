import requests
from bs4 import BeautifulSoup
import csv
import math
import os
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
import pandas as pd

# --- Constants ---
BASE_URL = "https://tpr.fmcsa.dot.gov"
PAGE_URL = BASE_URL + "/Provider/InReview"
API_URL = BASE_URL + "/api/Public/InReviewPublic"
PAGE_SIZE = 100  # Number of records per page
MASTER_FILE = "master_fmcsa.csv"  # Master file with all weekly snapshots
today = datetime.today().strftime("%Y-%m-%d")
SNAPSHOT_FILE = f"fmcsa_in_review_{today}.csv"  # Weekly snapshot file

# --- Setup session with retries ---
session = requests.Session()
retry_strategy = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)


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


def main():
    token = get_verification_token()

    # Fetch first page to get total records
    print("Fetching first page to determine total rows...")
    first = fetch_page(start=0, length=PAGE_SIZE, token=token)
    total_records = first["recordsTotal"]
    total_pages = math.ceil(total_records / PAGE_SIZE)
    print(f"Total records: {total_records} | Total pages: {total_pages}")

    all_rows = first["data"]

    # Fetch remaining pages with progress bar
    for page in tqdm(range(1, total_pages), desc="Fetching pages"):
        start = page * PAGE_SIZE
        page_json = fetch_page(start=start, length=PAGE_SIZE, token=token)
        all_rows.extend(page_json["data"])

    print(f"Total rows downloaded: {len(all_rows)}")

    # Convert to DataFrame
    df = pd.DataFrame(all_rows)
    df['scrape_date'] = today  # Add scrape_date column

    # --- Save weekly snapshot ---
    df.to_csv(SNAPSHOT_FILE, index=False)
    print(f"Saved weekly snapshot CSV: {SNAPSHOT_FILE}")

    # --- Append to master file ---
    if os.path.exists(MASTER_FILE):
        master_df = pd.read_csv(MASTER_FILE)
        master_df = pd.concat([master_df, df], ignore_index=True)
    else:
        master_df = df.copy()

    master_df.to_csv(MASTER_FILE, index=False)
    print(f"Updated master CSV: {MASTER_FILE}")

    # --- Optional: Diff report ---
    if os.path.exists(MASTER_FILE):
        try:
            previous_snapshot_file = sorted([f for f in os.listdir('.') if f.startswith("fmcsa_in_review_") and f.endswith(".csv") and f != SNAPSHOT_FILE])[-1]
            previous = pd.read_csv(previous_snapshot_file)
            current_set = set(df['Name'])
            previous_set = set(previous['Name'])
            new_entries = current_set - previous_set
            removed_entries = previous_set - current_set
            print(f"\nNew entries since last scrape ({previous_snapshot_file}): {len(new_entries)}")
            print(f"Removed entries since last scrape: {len(removed_entries)}")
        except IndexError:
            print("No previous snapshot to compare for diff report.")


if __name__ == "__main__":
    main()
