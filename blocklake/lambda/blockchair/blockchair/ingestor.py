from datetime import datetime
from abc import ABC, abstractmethod

from blockchair.api import Transactions
from blockchair.db import CheckpointModel, DynamoCheckpoints

class DataIngestor(ABC):
    def __init__(self, writer, coins) -> None:
        self.writer = writer
        self.coins = coins
        self.default_start_date = datetime.utcnow()
        self._checkpoint = None

    def _checkpoint_filename(self, coin) -> str:
        return f"{self.__class__.__name__}{coin}.checkpoint"

    def _write_checkpoint(self, coin) -> str:
        with open(self._checkpoint_filename(coin=coin), "w") as file:
            file.write(f"{self._checkpoint}")

    def _load_checkpoint(self, coin):
        try:
            with open(self._checkpoint_filename(coin=coin), "r") as f:
                return datetime.strptime(f.read(), "%Y-%m-%d %H:%M:%S")
        except FileNotFoundError:
            return self.default_start_date

    def _update_checkpoint(self, value, coin) -> None:
        self._checkpoint = value
        self._write_checkpoint(coin)

    @abstractmethod
    def ingest(self) -> None:
        pass


class AwsDataIngestor:
    def __init__(self, writer, coins) -> None:
        self.dynamo_checkpoint = DynamoCheckpoints(
            model=CheckpointModel,
            report_id=self.__class__.__name__,
            default_start_date=datetime.utcnow(),
        )
        self.writer = writer
        self.coins = coins
        self.default_start_date = datetime.utcnow()
        self._checkpoint = self._load_checkpoint()

    def _write_checkpoint(self):
        self.dynamo_checkpoint.create_checkpoint(checkpoint_date=self._checkpoint)

    def _load_checkpoint(self) -> datetime.date:
        return self.dynamo_checkpoint.get_checkpoint()

    def _update_checkpoint(self, value):
        self._checkpoint = value
        self.dynamo_checkpoint.create_or_update_checkpoint(
            checkpoint_date=self._checkpoint
        )

    @abstractmethod
    def ingest(self) -> None:
        pass


class AwsBlocksIngestor(AwsDataIngestor):
    def ingest(self) -> None:
        for coin in self.coins:
            date = self._load_checkpoint()
            if date < self.default_start_date:
                api = Transactions(coin=coin)
                data = api.get_data(date=date.strftime("%Y-%m-%d %H:%M:%S"))
                self.writer(coin=coin, api=api.type, date=date).write(data)
                self._update_checkpoint(data.tail(1).time.values[0])


class TransactionsIngestor(DataIngestor):
    def ingest(self) -> None:
        for coin in self.coins:
            date = self._load_checkpoint(coin)
            if date < self.default_start_date:
                api = Transactions(coin=coin)
                data = api.get_data(date=date.strftime("%Y-%m-%d %H:%M:%S"))
                self.writer(coin=coin, api=api.type, date=date).write(data)
                self._update_checkpoint(data.tail(1).time.values[0], coin)