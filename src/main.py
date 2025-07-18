import logging

from .crawler import WandbLogCrawler
from src.config import PROJECT_NAME, ENTITY_NAME, STORE_PATH

def main():
    logger = logging.getLogger("wandb_crawler")
    crawler = WandbLogCrawler(
        PROJECT_NAME,
        ENTITY_NAME,
        STORE_PATH,
        logger=logger,
    )
    crawler.run()


if __name__ == "__main__":
    main()