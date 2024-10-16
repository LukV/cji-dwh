from dagster import resource
from pandas import DataFrame
from authlib.integrations.httpx_client import OAuth2Client

class LinkedDataAPI:
    """
    LinkedDataAPI is a class that provides methods to interact 
    with the Linked Data API of UiTwisselingsplatform.
    """
    def __init__(self, client_id, client_secret, token_url, data_endpoint, query):
        # Initialize OAuth2Session without client_secret
        self.client = OAuth2Client(
            client_id=client_id,
            client_secret=client_secret,
            scope='profile email openid')

        self.client.fetch_token(token_url)
        
        self.data_endpoint = data_endpoint
        self.query = query

    def fetch_data(self):
        """Fetch data from the Linked Data API."""
        response = self.client.post(self.data_endpoint, data={'query': self.query}, timeout=None)
        
        if response.status_code == 200:
            data = response.json()
            df = DataFrame(data['results']['bindings'])
            # Process DataFrame to extract the 'value' fields
            return df.applymap(lambda x: x['value'] if isinstance(x, dict) else x)

        print(f'Error: {response.status_code} - {response.text}')
        return None

@resource(config_schema={
    "client_id": str,
    "client_secret": str,
    "token_url": str,
    "data_endpoint": str,
    "query": str
})
def linked_data_api_resource(context):
    """Dagster resource that provides the LinkedDataAPI."""
    return LinkedDataAPI(
        client_id=context.resource_config["client_id"],
        client_secret=context.resource_config["client_secret"],
        token_url=context.resource_config["token_url"],
        data_endpoint=context.resource_config["data_endpoint"],
        query=context.resource_config["query"]
    )
