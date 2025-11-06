# main.py
import json
import os
from .openbrewery_operator import OpenBreweryOperator

# Define where to save the output file (e.g., in a 'raw_data' directory)
# For simplicity, we'll save it one level up in core_insights
OUTPUT_FILE = os.path.join(os.getcwd(), 'raw_breweries.json')

def run_ingestion(state: str):
    """
    Main function to run the brewery data ingestion process.
    """
    operator = OpenBreweryOperator()
    
    # 1. Fetch data using the operator
    breweries_data = operator.fetch_breweries(state=state, per_page=10) # Fetch 10 for testing

    if breweries_data:
        # 2. Print a sample for verification
        print("\n--- Ingestion Verification ---")
        print(f"Total records fetched: {len(breweries_data)}")
        print(f"Saving to file: {OUTPUT_FILE}")
        
        # 3. Save the data (Simulating the Load step of ELT)
        try:
            with open(OUTPUT_FILE, 'w') as f:
                json.dump(breweries_data, f, indent=4)
            print(" successfully saved to raw_breweries.json")
        except Exception as e:
            print(f"Failed to save file: {e}")
            
    else:
        print("Ingestion aborted: No data fetched or error occurred.")


if __name__ == "__main__":
    # You can change the state here to test different regions
    # Run the ingestion for California breweries
    run_ingestion(state="california")