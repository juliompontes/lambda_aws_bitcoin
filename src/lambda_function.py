import datetime
import logging

from src.ingestors import AwsDaySummaryIngestor
from src.writers import S3Writer

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def lambda_handler(event, context):
    logger.info(f"{event}")
    logger.info(f"{context}")

    aws_day_summary_ingestor = AwsDaySummaryIngestor(
        writer=S3Writer,
        coins=["BTC", "ETH", "LTC"],
        default_start_date=datetime.date(2023, 1, 1)
    ).ingest()