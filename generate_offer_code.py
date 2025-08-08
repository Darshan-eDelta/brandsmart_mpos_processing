# generate_offer_code.py
import random
import sys
from log import log
# --- Refactored Imports ---
from sqlalchemy import create_engine, text
from conn import get_db_connection_string # Use the new connection logic

# --- Create a Single, Global Engine ---
# This engine uses your conn.py logic and will be used for all DB operations.
DB_URL = get_db_connection_string()
ENGINE = create_engine(DB_URL)

def generate_single_code() -> str:
    """Generates one random, 6-character alphanumeric code."""
    # Renamed 'len' to 'code_length' to avoid shadowing the built-in function
    code_length = 6
    chars = "ABCDFGHJKLMNPQRSTUVWXYZ"
    nums = "23456789"
    
    num_count = random.randint(1, 5)
    char_count = code_length - num_count
    
    # Generate the characters and numbers
    code_chars = [random.choice(chars) for _ in range(char_count)]
    code_nums = [random.choice(nums) for _ in range(num_count)]
    
    # Combine and shuffle
    combined_list = code_chars + code_nums
    random.shuffle(combined_list)
    
    return "".join(combined_list)


def generate_candidate_codes(n: int) -> set:
    """Generates a set of n unique candidate offer codes in memory."""
    offercodes = set()
    # Using a while loop is correct here to ensure exactly n unique codes are generated
    while len(offercodes) < n:
        offercodes.add(generate_single_code())
    return offercodes


def check_offercode_db(codes_to_check: set) -> set:
    """
    Checks a set of offer codes against the database and returns only the ones
    that DO NOT already exist. This function is now secure and efficient.
    """
    if not codes_to_check:
        return set()

    # This query is now safe from SQL injection and uses an efficient
    # PostgreSQL array comparison (`= ANY(...)`).
    query = text("""
        SELECT landing_page_offer_code 
        FROM mpos_post_sale_marketing 
        WHERE landing_page_offer_code = ANY(:codes)
    """)
    
    try:
        with ENGINE.connect() as conn:
            # Execute the query with safe parameters
            result = conn.execute(query, {"codes": list(codes_to_check)})
            existing_codes = {row[0] for row in result}
        
        # Return the codes that are not in the database using a set difference
        return codes_to_check - existing_codes
        
    except Exception as e:
        print(f"Database error while checking offer codes: {e}", file=sys.stderr)
        # In case of a DB error, assume all codes are invalid to be safe
        return set()


def generate_offer_code(n: int) -> list:
    """
    Generates a list of n unique offer codes that are guaranteed
    not to exist in the database.
    """
    final_codes = set()
    
    # Loop until we have collected the required number of unique, valid codes
    while len(final_codes) < n:
        needed = n - len(final_codes)
        
        # Generate a batch of new candidate codes
        candidate_codes = generate_candidate_codes(needed)
        
        # Check which of the candidates are valid (not in the DB)
        valid_codes = check_offercode_db(candidate_codes)
        
        # Add the valid codes to our final set
        final_codes.update(valid_codes)
        
        log.info(f"Generated {len(valid_codes)} new valid codes. Total collected: {len(final_codes)}/{n}")

    log.info(f"Finished. Total generated unique offer codes: {len(final_codes)}")
    return list(final_codes)


if __name__ == '__main__':
    # Example: generate 10 unique codes
    generated_codes = generate_offer_code(10)
    print("\nFinal Codes:")
    print(generated_codes)