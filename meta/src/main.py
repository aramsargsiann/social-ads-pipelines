 ! gcloud config set ai/disable_runtime_template_creation True
 !pip install facebook-business
 !pip install requests pandas
 !pip install concurrent.futures
import json
import time
import traceback
from datetime import datetime, timedelta
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.exceptions import FacebookRequestError
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib

# Configuration
class Config:
    # FB_TOKEN_DEFAULT = 'YOUR TOKEN'
    # FB_TOKEN_USA = 'YOUR TOKEN'

    TOKEN_MAP = {
        '1315317622252931': FB_TOKEN_USA
    }

    AD_ACCOUNT_IDS = [
        '1315317622252931',  # USA account
        '656425320025498', '698296588516869', '3994994933897819',
        '1367738127363899', '126024086140766', '2750680778422359',
        '1379217059653177', '817519435786315', '179863137014141',
        '3689168137992145', '1117863395937500', '164131495585396',
        '711341112637192', '293616828848688', '542409925017383',  '817519435786315'
    ]

    ATTRIBUTION_WINDOWS = ['7d_click']
    MAX_ACCOUNT_FAILURES = 3


class DeduplicationManager:
    def __init__(self):
        self.seen_records = set()

    def generate_record_hash(self, record):
        hash_data = {
            'adset_id': record.get('adset_id'),
            'ad_id': record.get('ad_id'),
            'date_start': record.get('date_start'),
            'date_stop': record.get('date_stop'),
            'account_id': record.get('account_id'),
            'attribution_window': record.get('attribution_window'),
            'country': record.get('country')
        }
        return hashlib.md5(json.dumps(hash_data, sort_keys=True).encode()).hexdigest()

    def is_duplicate(self, record):
        record_hash = self.generate_record_hash(record)
        if record_hash in self.seen_records:
            return True
        self.seen_records.add(record_hash)
        return False


class TokenValidator:
    @staticmethod
    def validate_token(token):
        try:
            FacebookAdsApi.init(access_token=token)
            test_account = AdAccount('act_1315317622252931')
            test_account.api_get(fields=['id', 'name'])
            return True, "Token is valid"
        except FacebookRequestError as e:
            error_msg = f"Token validation failed: {e.api_error_code} - {e.api_error_message}"
            return False, error_msg
        except Exception as e:
            return False, f"Token validation error: {str(e)}"


class EnhancedFacebookAPIClient:
    def __init__(self, account_id):
        self.account_id = account_id
        token = Config.TOKEN_MAP.get(account_id, Config.FB_TOKEN_DEFAULT)

        is_valid, message = TokenValidator.validate_token(token)
        if not is_valid:
            raise Exception(f"Invalid token for account {account_id}: {message}")

        token_type = "USA" if token == Config.FB_TOKEN_USA else "DEFAULT"
        print(f"🔑 Initializing account {account_id} with {token_type} token - Validated ✓")

        self.api = FacebookAdsApi.init(access_token=token)
        self.ad_account = AdAccount(f"act_{account_id}", api=self.api)


class EnhancedAdsetDataFetcher:
    def __init__(self, fb_client, deduplicator):
        self.fb_client = fb_client
        self.deduplicator = deduplicator
        self.max_poll_attempts = 40
        self.poll_interval = 15

    def fetch_all_attribution_windows(self, time_range):
        """Fetch data for all attribution windows"""
        all_data = []
        for window in Config.ATTRIBUTION_WINDOWS:
            print(f"\n--- Fetching for Attribution Window: {window} ---")
            window_data = self.fetch_raw_adset_data(time_range, window)
            if window_data:
                all_data.extend(window_data)
            time.sleep(1)  # Small delay between windows
        return all_data

    def _poll_async_job(self, async_job):
        try:
            job_id = async_job.get(AdReportRun.Field.id) or async_job.get('report_run_id')
            if not job_id:
                print("❌ Error: Could not get job ID from async_job")
                return False

            print(f"🔍 Polling async job ID: {job_id}")
            for attempt in range(self.max_poll_attempts):
                try:
                    current_job_state = AdReportRun(job_id, api=self.fb_client.api).api_get(fields=[
                        AdReportRun.Field.async_status,
                        AdReportRun.Field.async_percent_completion,
                    ])

                    status = current_job_state.get(AdReportRun.Field.async_status, 'Unknown')
                    percent = current_job_state.get(AdReportRun.Field.async_percent_completion, 0)

                    print(f"📊 Job {job_id}: {status} ({percent}%) [Attempt {attempt + 1}/{self.max_poll_attempts}]")

                    if status == 'Job Completed':
                        async_job._data.update(current_job_state._data)
                        print(f"✅ Async job {job_id} completed successfully")
                        return True

                    if status in ['Job Failed', 'Job Skipped']:
                        print(f"❌ Async job {job_id} failed: {status}")
                        return False

                    time.sleep(self.poll_interval)

                except Exception as e:
                    print(f"⚠️ Error polling job {job_id} (attempt {attempt + 1}): {str(e)}")
                    time.sleep(self.poll_interval)

            print(f"⏰ Async job {job_id} timed out after {self.max_poll_attempts} attempts")
            return False

        except Exception as e:
            print(f"💥 Critical error in polling: {str(e)}")
            return False

    def fetch_raw_adset_data(self, time_range, attribution_window):
        print(f"\n📅 Starting data fetch for time range: {time_range}, window: {attribution_window}")

        # Comprehensive fields list
        safe_fields = [
            'adset_name', 'adset_id', 'campaign_id', 'campaign_name',
            'ad_id', 'ad_name', 'account_id', 'date_start', 'date_stop',
            'impressions', 'clicks', 'spend', 'reach', 'frequency',
            'unique_clicks', 'inline_link_clicks', 'actions', 'action_values',
            'unique_outbound_clicks', 'video_avg_time_watched_actions',
            'video_p100_watched_actions', 'video_p25_watched_actions',
            'video_p50_watched_actions', 'objective'
        ]

        params = {
            'level': 'ad',
            'time_range': time_range,
            'time_increment': 1,
            'fields': safe_fields,
            'action_attribution_windows': [attribution_window],
            'breakdowns': ['country']
        }

        try:
            print("🚀 Starting async job for insights...")
            async_job = self.fb_client.ad_account.get_insights(params=params, is_async=True)

            if not self._poll_async_job(async_job):
                print("❌ Failed to complete async job")
                return None

            results = async_job.get_result()
            print(f"✅ Successfully retrieved {len(results) if results else 0} records")

            if not results:
                print("⚠️ No data returned from API")
                return []

            processed_data = []
            for insight in results:
                record = self._process_insight_record(insight)
                record['attribution_window'] = attribution_window

                if not self.deduplicator.is_duplicate(record):
                    processed_data.append(record)

            return processed_data

        except FacebookRequestError as e:
            print(f"❌ Facebook API Error: {e.api_error_code} - {e.api_error_message}")
            return None
        except Exception as e:
            print(f"💥 Unexpected error: {str(e)}")
            traceback.print_exc()
            return None

    def _process_insight_record(self, insight):
        """Process individual insight record and calculate derived metrics"""
        record = dict(insight)

        # Initialize purchase metrics
        record['purchases'] = 0
        record['purchase_value'] = 0.0
        record['post_shares'] = 0
        record['view_content'] = 0

        # Process actions and action_values
        actions = record.get('actions', [])
        action_values = record.get('action_values', [])

        for action in actions:
            if action.get('action_type') == 'post':
                record['post_shares'] = int(action.get('value', 0))
            if action.get('action_type') == 'purchase':
                record['purchases'] = int(action.get('value', 0))
            if action.get('action_type') == 'view_content':
                record['view_content'] = int(action.get('value', 0))

        for action_value in action_values:
            if action_value.get('action_type') == 'purchase':
                record['purchase_value'] = float(action_value.get('value', 0))
            if action_value.get('action_type') == 'view_content':
                record['view_content'] = float(action_value.get('value', 0))

        # Clean up
        record.pop('actions', None)
        record.pop('action_values', None)

        return record


def split_time_range(start_date_str, end_date_str, days=560):
    """Split time range into chunks for processing"""
    date_ranges = []
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()

    current_start = start_date
    while current_start <= end_date:
        chunk_end = min(current_start + timedelta(days=days - 1), end_date)
        date_ranges.append({
            "since": current_start.strftime("%Y-%m-%d"),
            "until": chunk_end.strftime("%Y-%m-%d")
        })
        current_start = chunk_end + timedelta(days=1)
    return date_ranges


def process_account(account_id, time_chunks, deduplicator):
    """Process a single account with all its time chunks"""
    account_data = []
    account_failures = 0

    try:
        fb_client = EnhancedFacebookAPIClient(account_id)
        fetcher = EnhancedAdsetDataFetcher(fb_client, deduplicator)

        for i, chunk in enumerate(time_chunks):
            print(f"\n📅 Processing chunk {i+1}/{len(time_chunks)} for account {account_id} - {chunk['since']} to {chunk['until']}")
            start_time = time.time()

            try:
                raw_data = fetcher.fetch_all_attribution_windows(chunk)

                if raw_data is None:
                    print("⚠️ No data returned for this chunk. Skipping.")
                    account_failures += 1
                else:
                    account_data.extend(raw_data)
                    print(f"✅ Successfully processed chunk. Total records so far: {len(account_data)}")

                elapsed = time.time() - start_time
                print(f"⏱️ Time for this chunk: {elapsed:.2f}s")

                # Add delay between chunks to avoid rate limiting
                if i < len(time_chunks) - 1:
                    print("⏳ Sleeping for 2 minutes before next chunk...")
                    time.sleep(120)  # 2 minutes delay

            except Exception as e:
                print(f"❌ Error processing chunk {i+1} for account {account_id}: {e}")
                account_failures += 1
                if account_failures >= Config.MAX_ACCOUNT_FAILURES:
                    print(f"⚠️ Too many failures for account {account_id}. Skipping remaining chunks.")
                    break

    except Exception as e:
        print(f"❌ Critical error initializing account {account_id}: {e}")
        return None

    return account_data


def save_data(account_id, data, combined_data):
    """Save data for individual account and add to combined dataset"""
    if not data:
        return

    # Add to combined data (with account ID for tracking)
    for record in data:
        record['source_account_id'] = account_id
    combined_data.extend(data)


def main():
    print("🚀 Starting Multi-Account Facebook Ads Data Fetcher")
    start_time = time.time()

    # Initialize deduplication manager
    deduplicator = DeduplicationManager()

    # Define your total range (10 days as requested)
    full_start = (datetime.now() - timedelta(days=403)).strftime('%Y-%m-%d')
    full_end = (datetime.now() - timedelta(days=393)).strftime('%Y-%m-%d')
    time_chunks = split_time_range(full_start, full_end)
    time_range_str = f"{full_start.replace('-', '_')}_to_{full_end.replace('-', '_')}"

    print(f"📅 Processing time range: {full_start} to {full_end}")
    print(f"📦 Number of time chunks: {len(time_chunks)}")

    combined_data = []
    successful_accounts = 0
    failed_accounts = []

    # Use ThreadPoolExecutor as requested
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(process_account, acc, time_chunks, deduplicator): acc
                  for acc in Config.AD_ACCOUNT_IDS}

        for future in as_completed(futures):
            account_id = futures[future]
            try:
                account_data = future.result()
                if account_data:
                    save_data(account_id, account_data, combined_data)
                    successful_accounts += 1
                    print(f"✔️ Successfully processed account {account_id}")
                else:
                    print(f"❌ No data returned for account {account_id}")
                    failed_accounts.append(account_id)
            except Exception as e:
                print(f"❌ Exception processing account {account_id}: {e}")
                failed_accounts.append(account_id)

    print(f"\nSuccessfully processed {successful_accounts} accounts.")
    if failed_accounts:
        print(f"Failed accounts: {failed_accounts}")
    print(f"Combined data collected: {len(combined_data)} records")

    # Save combined data as JSONL file
    if combined_data:
        combined_filename = f"facebook_combined_ad_data_{time_range_str}.jsonl"
        try:
            with open(combined_filename, 'w', encoding='utf-8') as f:
                 for record in combined_data:
                     json_line = json.dumps(record, ensure_ascii=False)
                     f.write(json_line + '\n')
            print(f"\n✅✅✅ Combined data saved: {combined_filename}")

            # Print sample of first record
            if combined_data:
                print(f"\n📄 Sample record structure:")
                print(json.dumps(combined_data[0], indent=2))

        except IOError as e:
            print(f"❌ Failed to save combined file {combined_filename}: {e}")

    # Summary report
    total_time = (time.time() - start_time) / 60  # in minutes
    print(f"\n{'='*80}")
    print("📊 Processing Summary:")
    print(f"✅ Successful accounts: {successful_accounts}/{len(Config.AD_ACCOUNT_IDS)}")
    print(f"❌ Failed accounts: {len(failed_accounts)}")
    if failed_accounts:
        print(f"Failed account IDs: {', '.join(failed_accounts)}")
    print(f"⏱️ Total processing time: {total_time:.1f} minutes")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
