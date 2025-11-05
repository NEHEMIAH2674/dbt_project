# africastalking_hook.py
from utils.api_hook import BaseApiHook
from dotenv import load_dotenv
import os

load_dotenv()

class AfricaTalkingApiHook(BaseApiHook):
    """
    Hook for connecting to Africa's Talking API using the API key.
    """

    def __init__(self, **kwargs):
        host = "https://api.africastalking.com/version1"
        api_key = os.getenv("AFRICASTALKING_API_KEY")
        username = os.getenv("AFRICASTALKING_USERNAME", "sandbox")

        if not api_key:
            raise ValueError("AFRICASTALKING_API_KEY must be set in .env file")

        headers = {
            "apiKey": api_key,
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        super().__init__(host=host, headers=headers, **kwargs)
        self.username = username
