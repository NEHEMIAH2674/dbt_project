# africastalking_operator.py
from data_bike.api.africastalking.africastalking_hook import AfricaTalkingApiHook
from utils.bq import BQConnector
from utils.log_config import logger
from datetime import datetime
import pandas as pd

class AfricaTalkingOperator:

    def __init__(self):
        self.hook = AfricaTalkingApiHook()
        self.dataset = "api_data"

    def execute(self, table_name: str):
        if table_name == "sms_logs":
            endpoint = f"messaging"
            data = self._extract_data(endpoint)
            self._load_to_bq(data, table_name)
        else:
            raise ValueError(f"Table {table_name} not supported")

    def _extract_data(self, endpoint: str):
        response = self.hook.get(endpoint)
        if not response or "SMSMessageData" not in response:
            logger.error("No SMS data found in response")
            return []

        messages = response["SMSMessageData"]["Messages"]
        for msg in messages:
            msg["fetched_at"] = datetime.now().isoformat()
        return messages

    def _load_to_bq(self, data, table_name):
        if not data:
            logger.warning("No data to load into BigQuery.")
            return

        bq = BQConnector()
        df = pd.DataFrame(data)
        bq.load_dataframe_to_table(df, dataset_name=self.dataset, table_name=table_name)
        logger.info(f"Loaded {len(df)} records into {table_name}")
