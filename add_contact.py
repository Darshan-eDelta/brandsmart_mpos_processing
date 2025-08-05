# add_contact.py
import os
import sys
import json
import requests
import time
from datetime import date

# --- Refactored Imports ---
from sqlalchemy import create_engine, text
from conn import get_db_connection_string # Use the new connection logic

# --- Existing Imports ---
from log import log
from ZohoTokenManager import ZohoTokenManager
from dotenv import load_dotenv

load_dotenv(override=True)

# --- Create a Single, Global Engine ---
# This engine uses your conn.py logic and will be used for all DB operations.
DB_URL = get_db_connection_string()
ENGINE = create_engine(DB_URL)

# --- Zoho Token Manager Setup (Unchanged) ---
ZOHO_CLIENT_ID = os.getenv('ZOHO_CLIENT_ID')
ZOHO_CLIENT_SECRET = os.getenv('ZOHO_CLIENT_SECRET')
ZOHO_REFRESH_TOKEN = os.getenv('ZOHO_REFRESH_TOKEN')

if not all([ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET, ZOHO_REFRESH_TOKEN]):
    log.error("Missing ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET, or ZOHO_REFRESH_TOKEN. Exiting.")
    sys.exit(1)

zoho_config = {
    "ZOHO_CLIENT_ID": ZOHO_CLIENT_ID,
    "ZOHO_CLIENT_SECRET": ZOHO_CLIENT_SECRET,
    "ZOHO_REFRESH_TOKEN": ZOHO_REFRESH_TOKEN,
}
zoho_token_manager = ZohoTokenManager(zoho_config)

# --- Zoho API Configuration & Rate Limiter (Unchanged) ---
ZOHO_API_BASE_URL = "https://campaigns.zoho.com/api/v1.1/json/listsubscribe"

class ZohoMARateLimiter:
    # This entire class is well-defined and remains unchanged.
    def __init__(self, calls_per_duration, duration_seconds, lock_period_seconds):
        self.calls_per_duration = calls_per_duration
        self.duration_seconds = duration_seconds
        self.lock_period_seconds = lock_period_seconds
        self.tokens = calls_per_duration
        self.last_refill_time = time.monotonic()
        self.locked_until_time = 0
        self.refill_rate = self.calls_per_duration / self.duration_seconds
        log.info(f"Zoho Rate Limiter: {calls_per_duration} calls/{duration_seconds}s. Refill: {self.refill_rate:.2f} tokens/sec.")

    def _refill_tokens(self):
        now = time.monotonic()
        time_elapsed = now - self.last_refill_time
        tokens_to_add = time_elapsed * self.refill_rate
        self.tokens = min(self.calls_per_duration, self.tokens + tokens_to_add)
        self.last_refill_time = now

    def acquire_token(self):
        while True:
            self._refill_tokens()
            now = time.monotonic()
            if now < self.locked_until_time:
                wait_time = self.locked_until_time - now
                log.warning(f"API locked. Waiting for {wait_time:.2f} seconds.")
                time.sleep(wait_time)
                continue
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            time_to_next_token = (1 - self.tokens) / self.refill_rate
            log.info(f"Rate limit hit, waiting {time_to_next_token:.2f} seconds.")
            time.sleep(time_to_next_token + 0.01)

    def trigger_lock(self):
        self.locked_until_time = time.monotonic() + self.lock_period_seconds
        log.error(f"Zoho API limit exceeded! Entering {self.lock_period_seconds}s lock.")
        self.tokens = 0
        self.last_refill_time = time.monotonic()

zoho_ma_rate_limiter = ZohoMARateLimiter(500, 300, 1800)


# The old execute_query and update_zoho_load_date functions are no longer needed.

# --- REFACTORED `import_contacts` Function ---
def import_contacts(batch_id: str):
    """
    Fetches records, adds them to Zoho, and then performs a single bulk
    update to the database for all successful contacts.
    """
    log.info(f"Starting contact import to Zoho for batch_id = {batch_id}")

    # Step 1: Fetch all records to be processed in a single query
    sql = text("""
        SELECT DISTINCT ON (landing_page_offer_code)
               customer_email, customer_first_name, customer_last_name,
               offer_code_url, landing_page_offer_code, campaign_start_date,
               manufacturer, department, campaign_duration
        FROM   mpos_post_sale_marketing
        WHERE  batch_id = :batch_id
          AND  customer_email IS NOT NULL AND customer_email <> ''
        ORDER BY landing_page_offer_code, id;
    """)
    try:
        with ENGINE.connect() as conn:
            records = conn.execute(sql, {"batch_id": str(batch_id)}).fetchall()
        log.info(f"Found {len(records)} unique contacts to process for batch {batch_id}.")
    except Exception as e:
        log.error(f"Failed to fetch records for batch {batch_id}: {e}")
        return

    if not records:
        return

    # Step 2: Loop through records, call the API, and collect successes
    successful_offer_codes = []
    for row in records:
        email, first_name, last_name, offer_code_url,offer_code, campaign_start_date, manufacturer, department, campaign_duration = row
        # This API interaction logic is unchanged
        lead_info = {
            "First Name": str(first_name) if first_name else '',
            "Last Name": str(last_name) if last_name else '',
            "Lead Email": email.split(';')[0].strip(),
            "contract_page_url":offer_code_url,
            "manufacturer": str(manufacturer) if manufacturer else '',
            "department": str(department) if department else '',
            "campaign_date": campaign_start_date.strftime("%m/%d/%Y") if campaign_start_date else '',
            "Is Converted": False,
            "emails_to_send": 2 if campaign_duration == 30 else 3 if campaign_duration == 60 else 4
        }
        try:
            zoho_ma_rate_limiter.acquire_token()
            access_token = zoho_token_manager.get_token()
            if not access_token:
                log.error(f"Failed to get Zoho token for '{email}'. Skipping.")
                continue

            headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
            payload = {"resfmt": "JSON", "leadinfo": json.dumps(lead_info)}

            response = requests.post(ZOHO_API_BASE_URL, headers=headers, data=payload)
            response.raise_for_status()
            zoho_response = response.json()

            if zoho_response.get('status') == "success":
                log.info(f"Successfully added '{email}' to Zoho (Offer: {offer_code}).")
                # Collect the offer code instead of updating the DB immediately
                successful_offer_codes.append(offer_code)
            else:
                log.error(f"Zoho API Error for '{email}': {zoho_response.get('message', 'Unknown')} (Offer: {offer_code})")

        except requests.exceptions.HTTPError as err:
            log.error(f"HTTP Error for '{email}': {err} - Response: {err.response.text}")
            if err.response.status_code == 429: zoho_ma_rate_limiter.trigger_lock()
        except Exception as err:
            log.error(f"Unexpected error for '{email}': {err}")

    # Step 3: Perform a single bulk UPDATE for all successful contacts
    if successful_offer_codes:
        log.info(f"Updating database for {len(successful_offer_codes)} successfully processed contacts...")
        update_sql = text("""
            UPDATE mpos_post_sale_marketing
            SET activity_zoho_campaign_load = :load_date
            WHERE landing_page_offer_code = ANY(:offer_codes)
        """)
        try:
            # .begin() ensures this bulk update is a single transaction
            with ENGINE.begin() as conn:
                conn.execute(update_sql, {
                    "load_date": date.today(),
                    "offer_codes": successful_offer_codes
                })
            log.info("Database update successful.")
        except Exception as e:
            log.error(f"Failed to perform bulk update on database: {e}")
    else:
        log.warning("No contacts were successfully processed to update in the database.")

    log.info(f"Finished contact import for batch_id = {batch_id}.")
# --- Main Entry Point (Unchanged) ---
def main(batch_id: str):
    """Main function to initiate adding contacts."""
    if not batch_id:
        log.error("No batch_id provided. Exiting.")
        return
    import_contacts(batch_id)

if __name__ == '__main__':
    # Default batch_id for testing purposes
    batch_to_process = sys.argv[1] if len(sys.argv) > 1 else None
    if batch_to_process:
        main(batch_to_process)
    else:
        log.error("Please provide a batch_id to process. Usage: python add_contact.py <batch_id>")