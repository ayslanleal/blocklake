# must be called as we're using zipped requirements
try:
    import unzip_requirements
except ImportError:
    pass

import requests
import boto3
import ratelimit
from backoff import on_exception, expo


class DeffiLama():

    def __init__(self, coins) -> None:
        self.coins = coins
        self.endpoint = "https://api.llama.fi/charts/{}"
        self.s3_client = boto3.client('s3')

    @on_exception(expo, ratelimit.exception.RateLimitException, max_tries=10)
    @ratelimit.limits(calls=29, period=30)
    @on_exception(expo, requests.exceptions.HTTPError, max_tries=10)
    def get_data(self):
        for coin in self.coins:

            r = requests.get(self.endpoint.format(coin))
            self.s3_client.put_object(
                Body=r.text,
                Bucket='block-lakehouse',
                Key=f'Deffilama/tvl/{coin}.json'
            )
    

def hello(event, context):
    
    chains = ['Optimism', 'Ethereum', 'Polygon', 'Celo', 'Arbitrum', 'Kava', 'Harmony', 
    'Moonbeam', 'Aurora', 'xDai', 'Avalanche', 'Fantom', 'Binance', 'Moonriver', 'Heco', 'Telos',
    'Fuse', 'OKExChain', 'Arbitrum Nova', 'Palm', 'Boba', 'Cronos', 'Oasis', 'Metis', 'Wanchain',
    'Solana', 'Terra', 'OntologyEVM', 'Ontology', 'Evmos', 'Astar', 'Velas', 'Kucoin', 'Milkomeda',
    'Bitcoin', 'Energi', 'Klaytn', 'Cosmos', 'Stafi', 'Kusama', 'Polkadot', 'IoTeX', 'Zilliqa',
    'NEO','Secret', 'Doge', 'Litecoin', 'RSK', 'Cube', 'Sifchain', 'Algorand', 'Elastos', 'REI',
    'ThunderCore', 'Nuls', 'Osmosis', 'Thorchain', 'Terra2', 'Hoo', 'Tron', 'Icon', 'Karura', 'EOS',
    'Wax', 'Tezos', 'Near', 'ORE', 'Ravencoin', 'Ultra', 'LBRY', 'Genshiro', 'Conflux', 'Lamden',
    'CLV', 'Waves', 'EnergyWeb', 'GoChain', 'HPB', 'Fusion', 'TomoChain', 'Kardia', 'SORA', 'EthereumPoW',
    'Crab', 'Dogechain', 'smartBCH', 'Cardano', 'EthereumClassic', 'Shiden', 'Ronin', 'zkSync', 'Polis',
    'ZYX', 'Elrond', 'Stellar', 'Songbird', 'Ubiq', 'Mixin', 'Everscale', 'VeChain', 'Callisto', 'Bittorrent',
    'Stacks', 'CSC', 'Godwoken', 'GodwokenV1', 'XDC', 'Hedera', 'Ergo', 'Nahmii', 'DefiChain', 'Heiko', 'Parallel',
    'Theta', 'Meter', 'Echelon', 'MultiVAC', 'Curio', 'Syscoin', 'Proton', 'Kadena', 'Kujira', 'Vite', 'DFK', 'Findora',
    'Hydra', 'Coti', 'Lachain', 'Bifrost', 'Juno', 'REIchain', 'Obyte', 'Acala', 'SXnetwork', 'ICP',
    'Nova Network', 'Kintsugi', 'Sora', 'Flow', 'Filecoin', 'Canto', 'Bitgert', 'Carbon', 'Starcoin', 'Ultron',
    'Tombchain', 'Crescent', 'Vision', 'Interlay', 'FunctionX']

    api = DeffiLama(chains)
    api.get_data()
