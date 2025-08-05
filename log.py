import logging
# import os
# from logdna import LogDNAHandler
# from dotenv import load_dotenv
# load_dotenv()
# logdna_key = os.getenv('logdna_key')
# logdna_key = None

# logdna_options = {
#         "app": "direct-marketing",
#         "hostname": "direct-marketing",
#         "index_meta": True,
#         "tags": ['direct-marketing'],
#     }

# logdna_handler = LogDNAHandler(logdna_key, options=logdna_options)
# logging.getLogger().addHandler(logdna_handler)
# logging.getLogger().setLevel(logging.INFO)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

log = logging