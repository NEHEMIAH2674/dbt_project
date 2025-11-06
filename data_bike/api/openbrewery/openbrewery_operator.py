# openbrewery_operator.py
import requests
from .openbrewery_hook import OpenBreweryApiHook

class OpenBreweryOperator:
    """
    Operator responsible for fetching data from the Open Brewery DB.
    """
    def __init__(self):
        # The operator instantiates the hook
        self.hook = OpenBreweryApiHook()
        self.endpoint = "/v1/breweries" # Define the primary endpoint

    def fetch_breweries(self, state: str = None, per_page: int = 50) -> list:
        """
        Fetches a list of breweries, optionally filtered by state.
        
        Args:
            state (str): Optional state to filter by.
            per_page (int): Number of results per page (max 200).
            
        Returns:
            list: A list of brewery dictionaries.
        """
        print(f"Fetching breweries for state: {state if state else 'ALL'}")
        
        # Build query parameters
        params = {
            "per_page": per_page
        }
        if state:
            params["by_state"] = state
            
        try:
            # Use the hook to run the request
            response = self.hook.run(self.endpoint, method="GET", params=params)

            if response.status_code == 200:
                print(f"Successfully fetched data (Status {response.status_code})")
                return response.json()
            else:
                print(f"API Request Failed: {response.status_code}")
                # Raise an exception to stop the process if ingestion fails
                response.raise_for_status() 
                
        except requests.exceptions.RequestException as e:
            print(f"Error during API call: {e}")
            return [] # Return empty list on failure
        
        return []