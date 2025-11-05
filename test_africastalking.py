from data_bike.api.africastalking.africastalking_hook import AfricaTalkingApiHook
from dotenv import load_dotenv
import json

load_dotenv()  # loads your .env with API key

def test_africastalking_connection():
    hook = AfricaTalkingApiHook()

    # This endpoint returns your account balance (safe for testing)
    endpoint = "user"
    params = {"username": hook.username}

    print("🔄 Sending request to Africa's Talking API...")
    response = hook.get(endpoint=endpoint, params=params)

    if response:
        print("✅ Connection successful! API responded:")
        print(json.dumps(response, indent=2))
    else:
        print("❌ No response. Check your API key or username in .env.")

if __name__ == "__main__":
    test_africastalking_connection()
