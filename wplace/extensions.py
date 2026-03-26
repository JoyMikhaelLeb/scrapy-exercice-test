import json
import logging
from datetime import datetime, timezone

from scrapy import signals

logger = logging.getLogger(__name__)


class JsonLoggingExtension:
    """
    Scrapy extension that:
    - Logs a JSON summary at the end of each run
    - Logs failed requests as JSON
    """

    def __init__(self):
        self.items_scraped   = 0
        self.items_failed    = 0
        self.failed_downloads = []

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls()
        crawler.signals.connect(ext.item_scraped,   signal=signals.item_scraped)
        crawler.signals.connect(ext.item_dropped,   signal=signals.item_dropped)
        crawler.signals.connect(ext.spider_closed,  signal=signals.spider_closed)
        return ext

    def item_scraped(self, item, spider):
        self.items_scraped += 1

    def item_dropped(self, item, spider):
        self.items_failed += 1

    def spider_closed(self, spider, reason):
        summary = {
            "event":            "run_summary",
            "timestamp":        datetime.now(timezone.utc).isoformat(),
            "start_date":       spider.start_date.isoformat(),
            "end_date":         spider.end_date.isoformat(),
            "bodies":           list(spider.bodies.keys()),
            "records_scraped":  self.items_scraped,
            "records_failed":   self.items_failed,
            "failed_downloads": self.failed_downloads,
            "finish_reason":    reason,
        }
        logger.info(json.dumps(summary))