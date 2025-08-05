import os
from dotenv import load_dotenv
load_dotenv()

# environment_list = { 1:'prod',2:'uat'}
# use_env = environment_list[2]
# use_env = os.getenv('use_env')

use_env = os.getenv('use_env', 'prod')
# QUOTAGUARDSTATIC_URL = os.getenv("QUOTAGUARDSTATIC_URL")

PG_PROD_DB_CONFIG = {
    'user': os.getenv('PG_PROD_USER', 'postgres_user_prod'),
    'password': os.getenv('PG_PROD_PASSWORD', 'postgres_password_prod'),
    'host': os.getenv('PG_PROD_HOST', 'localhost'),
    'port': os.getenv('PG_PROD_PORT', '5432'),
    'database': os.getenv('PG_PROD_DATABASE', 'your_pg_db_prod')
}

PG_DEV_DB_CONFIG = {
    'user': os.getenv('PG_DEV_USER', 'postgres_user_prod'),
    'password': os.getenv('PG_DEV_PASSWORD', 'postgres_password_prod'),
    'host': os.getenv('PG_DEV_HOST', 'localhost'),
    'port': os.getenv('PG_DEV_PORT', '5432'),
    'database': os.getenv('PG_DEV_DATABASE', 'your_pg_db_prod')
}