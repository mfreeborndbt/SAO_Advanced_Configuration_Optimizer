import json
import os
import requests


class DbtClient:
    """Client for querying dbt Cloud Discovery + Admin APIs."""

    def __init__(self, config):
        self.discovery_url = config["discovery_url"]
        self.environment_id = int(config["environment_id"])
        self.account_id = config["account_id"]
        self.project_id = config["project_id"]
        self.host_url = config["host_url"]
        self.token = config["token"]
        self.name = config.get("name", "unknown")
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        self.admin_headers = {
            "Authorization": f"Bearer {self.token}",
        }

    def query_discovery(self, graphql_query, variables=None):
        """Execute a GraphQL query against the Discovery API."""
        payload = {"query": graphql_query}
        if variables:
            payload["variables"] = variables
        response = requests.post(self.discovery_url, json=payload, headers=self.headers)
        response.raise_for_status()
        result = response.json()
        if "errors" in result:
            raise Exception(f"GraphQL errors: {result['errors']}")
        return result["data"]

    def admin_get(self, path, params=None):
        """GET request to the Admin API."""
        url = f"https://{self.host_url}/api/v2/accounts/{self.account_id}/{path}"
        resp = requests.get(url, params=params, headers=self.admin_headers)
        resp.raise_for_status()
        return resp.json()

    def admin_get_v3(self, path, params=None):
        """GET request to the Admin API v3."""
        url = f"https://{self.host_url}/api/v3/accounts/{self.account_id}/{path}"
        resp = requests.get(url, params=params, headers=self.admin_headers)
        resp.raise_for_status()
        return resp.json()

    def test_connection(self):
        """Verify connectivity."""
        query = """
        query ($environmentId: BigInt!) {
          environment(id: $environmentId) {
            dbtProjectName
            adapterType
            applied {
              lastUpdatedAt
              models(first: 3) {
                edges {
                  node { uniqueId }
                }
              }
            }
          }
        }
        """
        data = self.query_discovery(query, variables={"environmentId": self.environment_id})
        env = data["environment"]
        model_count = len(env["applied"]["models"]["edges"])
        print(f"[{self.name}] Connected to: {env['dbtProjectName']} ({env['adapterType']})")
        print(f"  Last updated: {env['applied']['lastUpdatedAt']}")
        print(f"  Sample models found: {model_count}")
        return True


CREDENTIALS_DIR = os.path.join(os.path.dirname(__file__), "config")
CREDENTIALS_PATH = os.path.join(CREDENTIALS_DIR, "credentials.json")


def load_credentials():
    """Read credentials from config/credentials.json. Returns dict or None."""
    if os.path.exists(CREDENTIALS_PATH):
        with open(CREDENTIALS_PATH) as f:
            data = json.load(f)
        # Validate that required keys are present
        required = ["host_url", "discovery_url", "account_id", "project_id", "environment_id", "token"]
        if all(data.get(k) for k in required):
            return data
    return None


def save_credentials(data):
    """Write credentials to config/credentials.json."""
    os.makedirs(CREDENTIALS_DIR, exist_ok=True)

    # Clean up common URL mistakes
    host = data.get("host_url", "").strip().rstrip("/")
    host = host.replace("https://", "").replace("http://", "")
    data["host_url"] = host

    disc = data.get("discovery_url", "").strip().rstrip("/")
    if disc and not disc.endswith("/graphql"):
        disc = disc + "/graphql"
    data["discovery_url"] = disc

    # Strip whitespace from all string fields
    for k in ["account_id", "project_id", "environment_id", "token"]:
        if data.get(k):
            data[k] = data[k].strip()

    # Auto-generate a name from the account URL
    if not data.get("name"):
        data["name"] = host.split(".")[0] if "." in host else host

    with open(CREDENTIALS_PATH, "w") as f:
        json.dump(data, f, indent=2)


def get_client_from_config():
    """Load credentials and return a DbtClient, or None if not configured."""
    creds = load_credentials()
    if creds is None:
        return None
    return DbtClient(creds)


if __name__ == "__main__":
    creds = load_credentials()
    if creds:
        try:
            client = DbtClient(creds)
            client.test_connection()
        except Exception as e:
            print(f"Connection failed: {e}")
    else:
        print("No credentials configured. Visit /setup to configure.")
