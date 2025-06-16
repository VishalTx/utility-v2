from jira import JIRA
from jira.exceptions import JIRAError
from utils.aws_s3_bucket import AWSDeluxeD3S3Bucket as Props

def test_jira_connectivity(jira_url, email, api_token):
    try:
        jira = JIRA(server=jira_url, basic_auth=(email, api_token))
        user = jira.current_user()
        print(f"Connected to JIRA as: {user}")
        return True
    except JIRAError as e:
        print(f"JIRA connection failed: {e}")
        return False

# Example usage
if __name__ == "__main__":
    JIRA_URL = Props.get_property_value_by_key("Jira_URL")
    EMAIL = Props.get_property_value_by_key("Email_id")
    API_TOKEN = Props.get_property_value_by_key("Jira_Api_Token")

    test_jira_connectivity(JIRA_URL, EMAIL, API_TOKEN)

'''
import requests
from requests.auth import HTTPBasicAuth
from DeluxeD3.DeluxeD3.src.utils.aws_s3_bucket import AWSDeluxeD3S3Bucket as Props

def connect_to_jira(jira_url: str, email: str, api_token: str):
    """
    Checks Jira connectivity using basic auth with email and API token.
    """
    auth = HTTPBasicAuth(email, api_token)

    try:
        response = requests.get(
            f"{jira_url}/rest/api/3/myself",
            headers={"Accept": "application/json"},
            auth=auth
        )

        if response.status_code == 200:
            print("✅ Successfully connected to Jira.")
            print(f"User Info: {response.json().get('displayName')}")
            return True
        else:
            print(f"❌ Failed to connect to Jira: {response.status_code} - {response.text}")
            return False

    except Exception as e:
        print(f"❌ Error while connecting to Jira: {str(e)}")
        return False


if __name__ == "__main__":
    # Replace with your Jira instance values
    JIRA_URL = Props.get_property_value_by_key("Jira_URL")
    EMAIL = Props.get_property_value_by_key("Email_id")
    API_TOKEN = Props.get_property_value_by_key("Jira_Api_Token")

    connect_to_jira(JIRA_URL, EMAIL, API_TOKEN)

'''