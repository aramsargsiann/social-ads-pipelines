!pip install requests python-dotenv
! gcloud config set ai/disable_runtime_template_creation True
!pip install facebook-business
!pip install google-cloud-bigquery-datatransfer
!pip install requests pandas google-cloud-bigquery
!pip install pandas-gbq


import os
import json
import time
import logging
from datetime import datetime, timedelta
from six import string_types
from six.moves.urllib.parse import urlencode, urlunparse

import requests
from dotenv import load_dotenv

load_dotenv()

# ---------- CONFIG ----------
ACCESS_TOKEN = os.getenv("TIKTOK_ACCESS_TOKEN", "YOUR TOKEN")
ADVERTISER_IDS = json.loads(os.getenv("TIKTOK_ADVERTISER_IDS", '["ACCOUNT_ID", "ACCOUNT_ID"]'))
REPORT_DAYS = int(os.getenv("REPORT_DAYS", "1"))  # number of days to include, default = 1
# TikTok API paths (keep as-is)
BASE_PATH = "/open_api/v1.3/report/integrated/get/"
AD_PATH = "/open_api/v1.3/ad/get/"
CAMPAIGN_PATH = "/open_api/v1.3/campaign/get/"
ADGROUP_PATH = "/open_api/v1.3/adgroup/get/"

# ---------- LOGGING ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("tiktok_etl")

# ---------- HELPERS ----------
def get_default_date_range(days):
    # end = yesterday, start = end - (days-1)
    end_dt = datetime.utcnow() - timedelta(days=)
    start_dt = end_dt
    return start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")

def build_url(path, query=""):
    scheme, netloc = "https", "business-api.tiktok.com"
    return urlunparse((scheme, netloc, path, "", query, ""))

def get_json(url_path, params=None, max_retries=3):
    headers = {"Access-Token": ACCESS_TOKEN, "Content-Type": "application/json"}
    for attempt in range(max_retries):
        try:
            if params:
                # ensure list/dict params are JSON-encoded
                q = urlencode({k: v if isinstance(v, string_types) else json.dumps(v) for k, v in params.items()})
                url_with_params = build_url(url_path, q)
            else:
                url_with_params = build_url(url_path)
            resp = requests.get(url_with_params, headers=headers, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request error (attempt {attempt+1}/{max_retries}): {e}")
            if attempt == max_retries - 1:
                return {"code": -1, "message": str(e)}
            time.sleep(2 ** attempt)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return {"code": -1, "message": str(e)}
    return {"code": -1, "message": "Unknown error"}

def validate_response(resp, endpoint_name):
    if not isinstance(resp, dict):
        logger.error(f"Invalid response from {endpoint_name}")
        return False
    if resp.get("code") != 0:
        logger.error(f"API returned error for {endpoint_name}: {resp.get('message')}")
        return False
    return True

def fetch_paginated_data(endpoint_path, base_params, data_key="list", advertiser_id=""):
    all_data = []
    page = 1
    while True:
        params = {**base_params, "page": page}
        resp = get_json(endpoint_path, params)
        if not validate_response(resp, endpoint_path):
            break
        data_list = resp.get("data", {}).get(data_key, [])
        if not data_list:
            break
        all_data.extend(data_list)
        page_info = resp.get("data", {}).get("page_info", {})
        if page >= page_info.get("total_page", 1):
            break
        page += 1
        time.sleep(0.15)
    return all_data

# ---------- METADATA FETCH ----------
def fetch_campaign_mapping(advertiser_id, page_size=500):
    params = {
        "advertiser_id": advertiser_id,
        "page_size": page_size,
        "fields": ["campaign_id", "campaign_name", "objective_type"]
    }
    campaigns = fetch_paginated_data(CAMPAIGN_PATH, params, "list", advertiser_id)
    return {str(c["campaign_id"]): {"campaign_name": c.get("campaign_name"), "objective_type": c.get("objective_type")} for c in campaigns}

def fetch_adgroup_mapping(advertiser_id, page_size=500):
    params = {
        "advertiser_id": advertiser_id,
        "page_size": page_size,
        "fields": ["adgroup_id", "adgroup_name"]
    }
    adgroups = fetch_paginated_data(ADGROUP_PATH, params, "list", advertiser_id)
    return {str(a["adgroup_id"]): a.get("adgroup_name") for a in adgroups}

def fetch_ad_mapping(advertiser_id, page_size=500):
    params = {
        "advertiser_id": advertiser_id,
        "page_size": page_size,
        "fields": ["ad_id", "ad_name", "adgroup_id", "campaign_id", "operation_status"]
    }
    ads = fetch_paginated_data(AD_PATH, params, "list", advertiser_id)
    mapping = {}
    for a in ads:
        mapping[str(a["ad_id"])] = {
            "ad_name": a.get("ad_name"),
            "adgroup_id": str(a.get("adgroup_id")) if a.get("adgroup_id") is not None else None,
            "campaign_id": str(a.get("campaign_id")) if a.get("campaign_id") is not None else None,
            "status": a.get("operation_status")
        }
    return mapping

def fetch_ad_metrics(advertiser_id, start_date, end_date, metrics, page_size=500):
    params = {
        "advertiser_id": advertiser_id,
        "report_type": "BASIC",
        "data_level": "AUCTION_AD",
        "service_type": "AUCTION",
        "start_date": start_date,
        "end_date": end_date,
        "metrics": metrics,
        "dimensions": ["ad_id", "stat_time_day", "country_code"],
        "page_size": page_size,
        "order_field": "spend",
        "order_type": "DESC",
        "attribution_window": "7d_click_1d_view",
        "filtering": [{
            "field_name": "ad_status",
            "filter_type": "IN",
            "filter_value": '["STATUS_ALL"]'
        }]
    }
    return fetch_paginated_data(BASE_PATH, params, "list", advertiser_id)

# ---------- MERGE & NORMALIZE ----------
def normalize_record(row, ad_mapping, campaign_mapping, adgroup_mapping, advertiser_id):
    from datetime import datetime
    expected_dims = ["campaign_name","campaign_id","adgroup_name","ad_id","ad_name",
                     "adgroup_id","status","stat_time_day","country_code","objective_type"]
    expected_metrics = ["complete_payment","total_complete_payment_rate","conversion","clicks","impressions","spend"]

    dims = dict(row.get("dimensions") or {})
    # Normalize ad_id keys to string for consistent lookup
    ad_id = dims.get("ad_id")
    ad_id_s = str(ad_id) if ad_id is not None else None

    mapping = ad_mapping.get(ad_id_s, {})  # may be empty
    campaign_id = mapping.get("campaign_id") or dims.get("campaign_id")
    campaign_info = campaign_mapping.get(str(campaign_id)) if campaign_id is not None else None

    # fill fields from mapping or existing dims, default to None
    dims["ad_name"] = mapping.get("ad_name") or dims.get("ad_name")
    dims["adgroup_id"] = mapping.get("adgroup_id") or dims.get("adgroup_id")
    dims["campaign_id"] = campaign_id or dims.get("campaign_id")
    dims["campaign_name"] = (campaign_info.get("campaign_name") if campaign_info else None) or dims.get("campaign_name")
    dims["objective_type"] = (campaign_info.get("objective_type") if campaign_info else None) or dims.get("objective_type")
    dims["adgroup_name"] = adgroup_mapping.get(dims.get("adgroup_id")) or dims.get("adgroup_name")
    dims["status"] = mapping.get("status") or dims.get("status")

    # normalize types: convert ids to int when possible (BigQuery expects INTEGER in schema)
    for id_field in ["campaign_id","adgroup_id","ad_id"]:
        val = dims.get(id_field)
        if val is not None:
            try:
                dims[id_field] = int(val)
            except Exception:
                # leave as string if cannot cast
                dims[id_field] = val

    # convert stat_time_day "YYYY-MM-DD" -> "YYYY-MM-DDT00:00:00Z" (BigQuery friendly TIMESTAMP)
    if dims.get("stat_time_day"):
        sd = dims["stat_time_day"]
        if isinstance(sd, str) and len(sd) == 10 and sd.count("-") == 2:
            try:
                d = datetime.strptime(sd, "%Y-%m-%d")
                dims["stat_time_day"] = d.strftime("%Y-%m-%dT00:00:00Z")
            except Exception:
                pass

    # ensure all expected dims exist
    for k in expected_dims:
        dims.setdefault(k, None)

    # metrics
    metrics = dict(row.get("metrics") or {})
    for m in expected_metrics:
        metrics.setdefault(m, None)

    # advertiser_id numeric if possible
    try:
        adv_id_val = int(advertiser_id)
    except Exception:
        adv_id_val = advertiser_id

    merged = {
        "dimensions": dims,
        "metrics": metrics,
        "advertiser_id": adv_id_val
    }
    return merged

def save_jsonl(records, filename):
    with open(filename, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

# ---------- MAIN ----------
def main():
    if not ACCESS_TOKEN or not ADVERTISER_IDS:
        logger.error("Missing TIKTOK_ACCESS_TOKEN or TIKTOK_ADVERTISER_IDS environment variables.")
        return 1

    start_date, end_date = get_default_date_range(REPORT_DAYS)
    METRICS = ["impressions", "clicks", "spend", "conversion", "complete_payment", "total_complete_payment_rate"]

    all_final = []
    for adv in ADVERTISER_IDS:
        logger.info(f"Processing advertiser {adv} for {start_date} to {end_date}")
        try:
            campaign_map = fetch_campaign_mapping(adv)
            adgroup_map = fetch_adgroup_mapping(adv)
            ad_map = fetch_ad_mapping(adv)
            raw_metrics = fetch_ad_metrics(adv, start_date, end_date, METRICS)

            # Merge and normalize (do NOT drop rows if metadata missing)
            for r in raw_metrics:
                merged = normalize_record(r, ad_map, campaign_map, adgroup_map, adv)
                all_final.append(merged)

            logger.info(f"Adv {adv}: metrics rows fetched {len(raw_metrics)} merged -> {len(all_final)} total")
        except Exception as e:
            logger.exception(f"Failed processing advertiser {adv}: {e}")
            continue
        time.sleep(0.5)

    # final file
    filename = f"tiktok_ad_report_{start_date}_to_{end_date}.jsonl"
    save_jsonl(all_final, filename)
    logger.info(f"Wrote {len(all_final)} rows to {filename}")
    # quick sanity prints
    if len(all_final) > 0:
        sample_dims = list(all_final[0]["dimensions"].keys())
        logger.info(f"Sample dimension keys: {sample_dims}")
        # check country_code presence fraction
        with_country = sum(1 for r in all_final if r["dimensions"].get("country_code") not in (None, "", []))
        logger.info(f"country_code present in {with_country}/{len(all_final)} rows")
    print(filename)
    return 0

if __name__ == "__main__":
    exit(main())
