from datetime import datetime
from data_bike.utils.general_utils import resolve_nested_key
from data_bike.utils.log_config import logger
from requests.auth import AuthBase
from requests.exceptions import RequestException
from dotenv import load_dotenv
import inspect
import json
import os
import requests
import time


class BaseApiHook:
    """Reusable API base class for CoreInsights ETL."""

    def __init__(
        self,
        host: str,
        auth: AuthBase | tuple | None = None,
        headers: dict | None = None,
        max_retries: int = 3,
        backoff_factor: int = 2,
        wait_time: int | None = None,
        **kwargs
    ):
        self.host = host.rstrip("/")
        self.auth = auth
        self.headers = headers or {}
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.wait_time = wait_time
        self.session = requests.Session()
        self.kwargs = kwargs

    # ---------------------------
    # Core request handling logic
    # ---------------------------
    def _request_with_retries(
        self,
        method: str,
        url: str,
        data_keypath: str | None = None,
        output_type: str = "json",
        **kwargs
    ):
        retries = 0
        backoff = 1

        while retries < self.max_retries:
            try:
                request_params = inspect.signature(self.session.request).parameters
                request_kwargs = {k: v for k, v in kwargs.items() if k in request_params}

                response = self.session.request(
                    method, url, auth=self.auth, headers=self.headers, **request_kwargs
                )

                response.raise_for_status()

                # Handle output formats
                if output_type == "json":
                    try:
                        json_response = response.json()
                        data = resolve_nested_key(json_response, data_keypath)
                    except json.decoder.JSONDecodeError:
                        logger.error(f"JSON decode failed for URL {url}")
                        data = None
                elif output_type == "text":
                    data = response.text
                else:
                    raise AttributeError(f"Unsupported output type '{output_type}'")

                return data

            except RequestException as e:
                retries += 1
                if retries >= self.max_retries:
                    logger.error(f"Request failed after {retries} attempts: {e}")
                    raise
                logger.warning(f"Retrying {method.upper()} {url} in {backoff} sec...")
                time.sleep(self.wait_time or backoff)
                backoff *= self.backoff_factor

    def get(self, endpoint: str, data_keypath: str | None = None, output_type="json", **kwargs):
        url = f"{self.host}/{endpoint.strip('/')}"
        return self._request_with_retries("get", url, data_keypath, output_type, **kwargs)