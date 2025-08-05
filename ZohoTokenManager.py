import requests
import logging
from datetime import datetime, timedelta

class ZohoTokenManager:
    def __init__(self, zoho_console_account):
        self.token = ""
        self.refresh_token_url = ""
        self.token_validity = timedelta(milliseconds=3400000)
        self.token_generated_time = datetime.now()

        if (zoho_console_account.get("ZOHO_REFRESH_TOKEN") and 
            zoho_console_account.get("ZOHO_CLIENT_ID") and 
            zoho_console_account.get("ZOHO_CLIENT_SECRET")):
            self.refresh_token_url = (
                f"https://accounts.zoho.com/oauth/v2/token"
                f"?refresh_token={zoho_console_account['ZOHO_REFRESH_TOKEN']}"
                f"&client_id={zoho_console_account['ZOHO_CLIENT_ID']}"
                f"&client_secret={zoho_console_account['ZOHO_CLIENT_SECRET']}"
                f"&grant_type=refresh_token"
            )
        else:
            logging.error("ZOHO_REFRESH_TOKEN , ZOHO_CLIENT_ID , ZOHO_CLIENT_SECRET are mandatory")

    def get_token(self):
        current_timestamp = datetime.now()
        difference_time = current_timestamp - self.token_generated_time
        
        if difference_time < self.token_validity and self.token:
            print(self.token)
            return self.token
        else:
            self.token = self.get_refreshed_token()
            print(self.token)
            return self.token

    def get_refreshed_token(self, retry=1):
        try:
            if retry > 3:
                return ""

            response = requests.post(self.refresh_token_url)
            response.raise_for_status()
            data = response.json()
            token = data.get("access_token")
            self.token_generated_time = datetime.now()
            return token
        except requests.RequestException as error:
            logging.error(f"Error fetching token: {error}")
            return self.get_refreshed_token(retry + 1)
