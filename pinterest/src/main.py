import requests
import json
import time

# ---------------------------
# ✅ CONFIG
# ---------------------------
ACCESS_TOKEN = "YOUR TOKEN"

AD_ACCOUNT_IDS = [
   # ADD YOUR ACCOUNT_IDS 
]


YESTERDAY1 = (datetime.utcnow() - timedelta(days=10)).date()
YESTERDAY2 = (datetime.utcnow() - timedelta(days=1)).date()
start_date = YESTERDAY2.isoformat()
end_date = YESTERDAY1.isoformat()

columns = ["CAMPAIGN_ID",
           "CAMPAIGN_NAME",
           "CAMPAIGN_OBJECTIVE_TYPE", 
           "AD_GROUP_ID", 
           "AD_GROUP_NAME",
           "AD_NAME",
           "SPEND_IN_DOLLAR",
            "TOTAL_IMPRESSION",
            "TOTAL_CLICKTHROUGH",
            "TOTAL_CHECKOUT", 
            "TOTAL_CHECKOUT_VALUE_IN_MICRO_DOLLAR"]
granularity = "DAY"
targeting_types = "COUNTRY"

HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

# ---------------------------
# ✅ HELPER: SPLIT LIST INTO CHUNKS
# ---------------------------
def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

# ---------------------------
# ✅ FETCH ADS WITH RETRIES
# ---------------------------
def fetch_ads_for_account(ad_account_id, page_size=100, max_retries=3):
    ad_ids = []
    url = f"https://api.pinterest.com/v5/ad_accounts/{ad_account_id}/ads"
    params = {"page_size": page_size}

    while url:
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=HEADERS, params=params)
                if response.status_code == 200:
                    break
                else:
                    wait = 2 ** attempt
                    print(f"⚠️ Attempt {attempt+1} failed ({response.status_code}), retrying in {wait}s...")
                    time.sleep(wait)
            except requests.exceptions.RequestException as e:
                wait = 2 ** attempt
                print(f"⚠️ Network error: {e}, retrying in {wait}s...")
                time.sleep(wait)
        else:
            print(f"❌ Failed to fetch ads after {max_retries} retries for {ad_account_id}")
            return ad_ids

        data = response.json()
        items = data.get("items", [])
        ad_ids.extend([ad["id"] for ad in items if "id" in ad])

        bookmark = data.get("bookmark")
        if bookmark:
            url = f"https://api.pinterest.com/v5/ad_accounts/{ad_account_id}/ads?bookmark={bookmark}"
            params = {"page_size": page_size}
            time.sleep(0.2)  # small delay
        else:
            url = None

    print(f"✅ {ad_account_id}: {len(ad_ids)} ads fetched")
    return ad_ids

# ---------------------------
# ✅ FETCH TARGETING ANALYTICS IN CHUNKS
# ---------------------------
def fetch_targeting_analytics_chunks(ad_account_id, ad_ids):
    if not ad_ids:
        print(f"⚠️ No ads for {ad_account_id}, skipping.")
        return []

    all_data = []

    for batch in chunks(ad_ids, 250):  # max 250 per Pinterest API
        url = f"https://api.pinterest.com/v5/ad_accounts/{ad_account_id}/ads/targeting_analytics"
        params = {
            "ad_account_id": ad_account_id,
            "ad_ids": ",".join(batch),
            "start_date": start_date,
            "end_date": end_date,
            "columns": ",".join(columns),
            "granularity": granularity,
            "targeting_types": targeting_types,
            "click_window_days": 7,
            "view_window_days": 1,
            "engagement_window_days": 30

        }

        for attempt in range(3):  # retries for analytics
            response = requests.get(url, headers=HEADERS, params=params)
            if response.status_code == 200:
                break
            else:
                wait = 2 ** attempt
                print(f"⚠️ Analytics attempt {attempt+1} failed ({response.status_code}), retrying in {wait}s...")
                time.sleep(wait)
        else:
            print(f"❌ Failed batch for {ad_account_id}, skipping batch.")
            continue

        try:
            data = response.json()
            for row in data.get("data", []):
                row["ad_account_id"] = ad_account_id
            all_data.extend(data.get("data", []))
            time.sleep(0.3)
        except json.JSONDecodeError:
            print(f"❌ Invalid JSON for {ad_account_id} batch")
            continue

    print(f"📊 {ad_account_id}: total rows fetched: {len(all_data)}")
    return all_data

# ---------------------------
# ✅ MAIN LOOP
# ---------------------------
combined_data = []

for account_id in AD_ACCOUNT_IDS:
    print(f"\n🚀 Processing Account: {account_id}")
    ad_ids = fetch_ads_for_account(account_id)
    analytics_data = fetch_targeting_analytics_chunks(account_id, ad_ids)
    combined_data.extend(analytics_data)

# ---------------------------
# ✅ SAVE COMBINED FILE
# ---------------------------
file_name = f"pin_promotion_flat_conversions_{start_date}.jsonl"
with open(file_name, "w") as f:
    for row in combined_data:
        json_line = json.dumps(row)
        f.write(json_line + "\n")

print(f"\n💾 Saved combined file: {file_name}")
print(f"✅ Total rows: {len(combined_data)}")
