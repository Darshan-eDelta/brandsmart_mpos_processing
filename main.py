# main_process.py
import os
import random
import pandas as pd
from datetime import datetime
from typing import Optional, Any, Union

# --- New Imports ---
from sqlalchemy import create_engine, text
from sqlalchemy.types import Date
from conn import get_db_connection_string # Import the new function

# --- Existing Project-Specific Imports ---
from add_contact import import_contacts
from generate_offer_code import generate_offer_code
from config import use_env
from log import log

# --- Create a Single, Global Engine ---
# This engine uses your conn.py logic and will be used for all DB operations.
DB_URL = get_db_connection_string()
ENGINE = create_engine(DB_URL)

# --- Helper Functions (Unchanged) ---
def UUID() -> int:
    """Generates a unique ID based on a timestamp and a random number."""
    now_str = datetime.now().strftime("%y%m%d%H%M%S%f")
    return int(f"{now_str[:-2]}{random.randint(0, 9)}")

def offer_code_url_f(dealer_id: Union[str, int], offer_code: str = "") -> str:
    """Returns the appropriate offer code URL based on dealer_id and environment."""
    dealer_id_str = str(dealer_id)
    base_url = os.getenv(f"{use_env}_brandsmart_url", "http://default-brandsmart-url.com")
    if dealer_id_str == '5779155000141100449':
        return f"{base_url}/{offer_code}"
    return f"http://default-general-url.com/{offer_code}"

def generate_offercode(df: pd.DataFrame) -> pd.DataFrame:
    """Generates and merges unique offer codes into the DataFrame."""
    if 'invoice_number' not in df.columns or df['invoice_number'].nunique() == 0:
        log.warning("No unique invoice numbers found to generate offer codes.")
        df['offer_code'] = None
        return df

    unique_invoices = df[['invoice_number']].drop_duplicates().reset_index(drop=True)
    offer_codes = generate_offer_code(len(unique_invoices))
    unique_invoices['offer_code'] = offer_codes
    df = pd.merge(df, unique_invoices, on='invoice_number', how='left')
    log.info("Successfully generated and merged offer codes.")
    return df

def get_val_or_none(val: Any) -> Optional[str]:
    """Helper to clean and convert values to string or return None."""
    if val is None or pd.isna(val) or (isinstance(val, str) and not val.strip()):
        return None
    return str(val).strip()

# --- REFACTORED `update_data` Function ---
def update_data(df: pd.DataFrame, batch_id: int):
    """
    Updates records by loading data into a temporary table, running a
    single UPDATE...FROM query, and then dropping the temporary table.
    """
    log.info(f"Starting high-performance database update for batch_id = {batch_id}")
    if df.empty:
        log.warning("Input DataFrame is empty. No updates to perform.")
        return

    update_tasks = df.drop_duplicates(subset=["invoice_number", "dealer_id"]).dropna(
        subset=['invoice_number', 'dealer_id']
    ).copy()

    log.info(f"Consolidated into {len(update_tasks)} unique update operations.")
    if update_tasks.empty:
        log.warning("No valid tasks after cleaning data. Exiting.")
        return

    upload_df = update_tasks[['invoice_number', 'dealer_id']].copy()

    upload_df['activity_plan_purchased_date'] = pd.NaT
    upload_df['landing_page_offer_code'] = update_tasks['offer_code'].apply(get_val_or_none)
    upload_df['batch_id'] = str(batch_id)
    upload_df['offer_code_url'] = update_tasks.apply(
        lambda row: offer_code_url_f(row.get('dealer_id'), row.get('offer_code')), axis=1
    )
    upload_df['needs_python_proccess'] = 0

    # Use the specified static temporary table name
    temp_table_name = "temp_update_mpos_remove"

    try:
        with ENGINE.begin() as conn:
            log.info(f"Loading {len(upload_df)} records into temporary table '{temp_table_name}'...")
            upload_df.to_sql(
                name=temp_table_name,
                con=conn,
                if_exists="replace",
                index=False,
                dtype={'activity_plan_purchased_date': Date}
            )
            log.info("Temporary table created and populated.")

            log.info("Executing the final UPDATE...FROM query.")
            update_query = text(f"""
                UPDATE mpos_post_sale_marketing AS main
                SET
                    activity_plan_purchased_date = temp.activity_plan_purchased_date,
                    landing_page_offer_code = temp.landing_page_offer_code,
                    batch_id = temp.batch_id,
                    offer_code_url = temp.offer_code_url,
                    needs_python_proccess = temp.needs_python_proccess
                FROM {temp_table_name} AS temp
                WHERE
                    main.invoice_number = temp.invoice_number
                    AND main.dealer_id = temp.dealer_id;
            """)
            result = conn.execute(update_query)
            log.info(f"UPDATE command sent. Rows affected: {result.rowcount}")

        log.info(f"Successfully committed all updates for batch {batch_id}.")

        # Verification step
        with ENGINE.connect() as conn:
            result = conn.execute(
                text('SELECT COUNT(DISTINCT(invoice_number)) FROM mpos_post_sale_marketing WHERE batch_id = :batch_id'),
                {"batch_id": str(batch_id)}
            ).scalar_one_or_none()
            log.info(f"DB verification for batch {batch_id}: Found {result or 0} unique invoices.")
        
        # --- THIS IS THE NEW PART ---
        # Clean up by dropping the temporary table after a successful run
        log.info(f"Cleaning up temporary table '{temp_table_name}'...")
        with ENGINE.connect() as conn:
            conn.execute(text(f"DROP TABLE {temp_table_name}"))
            log.info("Temporary table dropped successfully.")
        # --- END OF NEW PART ---

    except Exception as e:
        log.error(f"The bulk update failed and was rolled back. Error: {e}")
        # Note: If the script fails, the temp table might not be dropped here,
        # but `if_exists="replace"` will handle it on the next run.
        raise
def process_mpos_data():
    """Orchestrates the entire process using the global SQLAlchemy engine."""
    # General query
    # query = text("SELECT id, landing_page_offer_code, dealer_id, invoice_number FROM mpos_post_sale_marketing WHERE needs_python_proccess = '1'")
    # August query
    # query = text("SELECT id, landing_page_offer_code, dealer_id, invoice_number FROM mpos_post_sale_marketing WHERE inbound_batch_id = '21'")
    # September query
    # query = text("SELECT id, landing_page_offer_code, dealer_id, invoice_number FROM mpos_post_sale_marketing WHERE needs_python_proccess = '1' and inbound_batch_id = '61'")
    # October query
    # query = text("SELECT id, landing_page_offer_code, dealer_id, invoice_number FROM mpos_post_sale_marketing WHERE needs_python_proccess = '1' and inbound_batch_id = '63'")
    # November query
    # query = text("SELECT id, landing_page_offer_code, dealer_id, invoice_number FROM mpos_post_sale_marketing WHERE needs_python_proccess = '1' and inbound_batch_id = '65'")
    # December query
    # query = text("SELECT id, landing_page_offer_code, dealer_id, invoice_number FROM mpos_post_sale_marketing WHERE needs_python_proccess = '1' and inbound_batch_id = '67'")
    
    try:
        df = pd.read_sql(query, ENGINE.connect())
        log.info(f"Fetched {len(df)} records from database for processing.")
    except Exception as e:
        log.error(f"Failed to fetch initial data from the database. Error: {e}")
        return

    if not df.empty:
        batch_id = UUID()
        df_with_codes = generate_offercode(df)
        update_data(df_with_codes, batch_id)
        import_contacts(batch_id)
    else:
        log.warning("No records found that require processing.")

# --- Main Entry Point (Unchanged) ---
def main():
    """Main entry point for the script."""
    log.info("************** START PROCESS **************")
    process_mpos_data()
    log.info("************** END PROCESS **************")

if __name__ == '__main__':
    main()