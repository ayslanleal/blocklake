from datetime import datetime
import os
import pandas as pd
import boto3


class DataWriter:

    def __init__(self, coin: str, api: str, date: datetime) -> None:
        self.api = api
        self.coin = coin
        self.date = date.strftime("%Y%m%d")
        self.filename = f"blockchair_{self.coin}_{self.api}_{self.date}.tsv.gz"
        self.path = f"{self.api}/{self.coin}/"

    def write(self, response) -> None:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        df_response = response

        if os.path.isfile(self.path + self.filename):
            df = pd.read_csv(f"{self.path}{self.filename}")
            df_concat = pd.concat([df, df_response])
            df_concat.to_csv(self.path + self.filename, index=False, compression="gzip")
        else:
            df_response.to_csv(
                self.path + self.filename, index=False, compression="gzip"
            )


class S3Writer(DataWriter):
    def __init__(self, coin: str, api: str, date: datetime) -> None:
        super().__init__(coin, api, date)
        self.s3_client = boto3.client("s3")
        self.bucket_name = "block-lakehouse"

    def write(self, response) -> None:
        df_response = response
        if self._is_path_bucket():
            df = pd.read_csv(
                f"s3://{self.bucket_name}/{self.path}{self.filename}",
                sep="\t",
                compression="gzip",
            )
            df_concat = pd.concat([df, df_response])
            self._write_to_s3(df_concat)

        else:
            self._write_to_s3(df_response)

    def _write_to_s3(self, df):
        df.to_csv(
            f"s3://{self.bucket_name}/{self.path}{self.filename}",
            index=False,
            sep="\t",
            compression="gzip",
        )

    def _is_path_bucket(self):
        filepath = f"{self.path}{self.filename}"
        list_paths_bucket = self.s3_client.list_objects(
            Bucket=self.bucket_name, Prefix=self.path
        )

        for path in list_paths_bucket.get("Contents"):
            if path["Key"] == filepath:
                return True
            else:
                return False
