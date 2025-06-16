import requests
import json
from urllib.parse import urlencode
from utils.properties_util import properties_util
from utils.aws_s3_bucket import AWSDeluxeD3S3Bucket

class OkHttpAPI:
    def __init__(self, environment):
        environment = environment.lower().strip()
        if environment == "qa":
            print("QA Environment Config Setup Loaded")
            self.run_aws_api("qa")
            print("Properties File Updated with New 12 Hours Validity AWS Tokens For QA ENV ...")
        elif environment == "dev":
            print("DEV Environment Config Setup Loaded")
            self.run_aws_api("dev")
            print("Properties File Updated with New 12 Hours Validity AWS Tokens For DEV ENV ...")
        elif environment == "pre-prod":
            # Extend logic as needed
            pass
        elif environment == "prod":
            # Extend logic as needed
            pass
        else:
            print("Unknown environment specified.")

    def run_aws_api(self, environment):
        account_id = "590407490654" if environment == "dev" else "156151586753"
        role_name = (
            "ddm_dev_dataengineer_offshore"
            if environment == "dev"
            else "ddm_qa_dataengineer_offshore"
        )

        base_url = "https://portal.sso.us-east-1.amazonaws.com/federation/credentials"
        params = {
            "account_id": account_id,
            "role_name": role_name,
            "max-session-duration": "129600" # 36 hours; deluxe restrict to 12 hours maximum
        }

        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {AWSDeluxeD3S3Bucket.get_property_value_by_key('API_Access_Token')}"
        }

        print(f"{base_url}?{urlencode(params)}")
        try:
            response = requests.get(base_url, headers=headers, params=params)
            if response.status_code != 200:
                print("Couldn't access the Get API Request")
                return
            print("Could Access the Get API Request Successfully ")
            print("Response Code is :", response.status_code)

            json_data = response.json()
            credentials = json_data.get("roleCredentials", {})

            access_key = credentials.get("accessKeyId", "")
            secret_key = credentials.get("secretAccessKey", "")
            session_token = credentials.get("sessionToken", "")

            prefix = "DEV_" if environment == "dev" else "QA_"
            props = properties_util
            props.update_existing_property(prefix + "AWS_ACCESS_KEY_ID", access_key)
            props.update_existing_property(prefix + "AWS_SECRET_ACCESS_KEY", secret_key)
            props.update_existing_property(prefix + "AWS_SESSION_TOKEN", session_token)

        except requests.RequestException as e:
            raise RuntimeError(f"Request failed: {e}")


if __name__ == "__main__":
    OkHttpAPI("dev")
