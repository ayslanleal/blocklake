from backoff import on_exception, expo
from abc import ABC, abstractmethod
from datetime import datetime

import requests
import ratelimit
import logging
import pandas as pd


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class Blockchair(ABC):
    def __init__(self, coin: str) -> None:
        self._endpoint = "https://api.blockchair.com/"
        self.coin = coin
        self.offset = 0

    @abstractmethod
    def _get_endpoint(self) -> str:
        pass

    @on_exception(expo, ratelimit.exception.RateLimitException, max_tries=10)
    @ratelimit.limits(calls=29, period=30)
    @on_exception(expo, requests.exceptions.HTTPError, max_tries=10)
    def get_data(self, **kwargs):
        endpoint = self._get_endpoint(**kwargs)
        response = requests.get(endpoint)
        logger.info(f"Getting data from endpoint {endpoint}")
        response.raise_for_status()
        response_json = response.json()
        lista_df = [pd.DataFrame(response_json["data"])]

        for i in range(100, response_json["context"]["total_rows"], 100):
            self.offset = i
            endpoint = self._get_endpoint(**kwargs)
            response = requests.get(endpoint)
            logger.info(f"Getting data from endpoint {endpoint}")
            response.raise_for_status()
            response_json = response.json()
            lista_df.append(pd.DataFrame(response_json["data"]))

        dfs_contant = pd.concat(lista_df)
        return dfs_contant


class Transactions(Blockchair):

    query_left = "https://api.blockchair.com/bitcoin/transactions?q=time(2022-10-02%2000:02:08...)"
    type = "blocks"

    def _get_endpoint(self, date: datetime) -> str:
        endpoint = f"{self._endpoint}/{self.coin}/{self.type}?q=time({date}...)&s=time(asc)&limit=100&offset={self.offset}"
        return endpoint