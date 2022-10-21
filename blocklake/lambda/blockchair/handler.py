try:
    import unzip_requirements
except ImportError:
    pass

from blockchair.ingestor import AwsBlocksIngestor
from blockchair.writer import S3Writer

def hello(event, context):
    
    blocks = AwsBlocksIngestor(writer=S3Writer, coins=["bitcoin"])
    blocks.ingest()