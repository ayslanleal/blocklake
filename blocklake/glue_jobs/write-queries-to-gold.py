import boto3

class AthenaCall:

    def __init__(self, client, output, database) -> None:
        self.client = client
        self.output = output
        self.database = database


    def start_query(self):
        self.query_chains_total()
        self.query_select_all_porcentage()
        self.query_volume_on_chain()
        self.query_fee_total_usd()
        

    def query_chains_total(self):
        self.client.start_query_execution(
            QueryString=""" 
                            SELECT date,
                                Sum(totalliquidityusd) as total_tvl
                            FROM   "blocklake-catalog"."deffillama"
                            GROUP  BY date
                            ORDER  BY date ASC
                            """,
            QueryExecutionContext={
                'Database': self.database,
                'Catalog': 'AwsDataCatalog'
            },
            ResultConfiguration = {
                'OutputLocation': self.output
            }
        )


    def query_select_all_porcentage(self):
        self.client.start_query_execution(
            QueryString="""
                            SELECT *
                            FROM   "blocklake-catalog"."deffillama"
                            ORDER  BY date ASC""",
            QueryExecutionContext={
                'Database': self.database,
                'Catalog': 'AwsDataCatalog'
            },
            ResultConfiguration = {
                'OutputLocation': self.output
            }
        )

    def query_volume_on_chain(self):
        self.client.start_query_execution(
            QueryString="""
                            SELECT date_partition,
                                   Sum(Cast(output_total AS BIGINT)) AS Outuput_total_blocks
                            FROM   "blocklake-catalog"."bitcoin"
                            GROUP  BY date_partition
                            ORDER  BY date_partition""",
            QueryExecutionContext={
                'Database': self.database,
                'Catalog': 'AwsDataCatalog'
            },
            ResultConfiguration = {
                'OutputLocation': self.output
            }
        )

    def query_fee_total_usd(self):
        self.client.start_query_execution(
            QueryString="""
                            SELECT date_partition,
                                   Sum(Cast(fee_total AS BIGINT)) AS Outuput_total_blocks
                            FROM   "blocklake-catalog"."bitcoin"
                            GROUP  BY date_partition
                            ORDER  BY date_partition""",
            QueryExecutionContext={
                'Database': self.database,
                'Catalog': 'AwsDataCatalog'
            },
            ResultConfiguration = {
                'OutputLocation': self.output
            }
        )


if __name__ == "__main__":
    client = boto3.client('athena', region_name='us-east-1')
    output='s3://block-lakehouse/gold/tables_csv/'
    database = "blocklake-catalog"
    
    athena = AthenaCall(client=client, output=output, database=database)
    athena.start_query()
