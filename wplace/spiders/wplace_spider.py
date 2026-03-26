import json
import scrapy
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from wplace.items import WplaceItem


# ── Constants ──────────────────────────────────────────────────────────────────

BODIES = {
    "Employment Appeals Tribunal":    2,
    "Equality Tribunal":              1,
    "Labour Court":                   3,
    "Workplace Relations Commission": 15376,
}

BASE_URL = "https://www.workplacerelations.ie/en/search/"


# ── Spider ─────────────────────────────────────────────────────────────────────

class WplaceSpider(scrapy.Spider):
    name = "wplace"
    allowed_domains = ["workplacerelations.ie"]

    def __init__(self, start_date=None, end_date=None, body=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not start_date or not end_date:
            raise ValueError("start_date and end_date are required. Format: DD-MM-YYYY")

        self.start_date = datetime.strptime(start_date, "%d-%m-%Y")
        self.end_date   = datetime.strptime(end_date,   "%d-%m-%Y")

        # optional: scrape only one body (useful for testing)
        if body:
            if body not in BODIES:
                raise ValueError(f"Invalid body. Choose from: {list(BODIES.keys())}")
            self.bodies = {body: BODIES[body]}
        else:
            self.bodies = BODIES

    async def start(self):
        """
        Entry point. Generates one request per (body × monthly partition).
        Example: 4 bodies × 12 months = 48 starting requests.
        """
        partitions = self._generate_monthly_partitions()

        for body_name, body_id in self.bodies.items():
            for partition_start, partition_end in partitions:
                url = self._build_url(
                    from_date=partition_start,
                    to_date=partition_end,
                    body_id=body_id,
                    page=1,
                )
                yield scrapy.Request(
                    url=url,
                    callback=self.parse,
                    meta={
                        "body":            body_name,
                        "body_id":         body_id,
                        "partition_start": partition_start.strftime("%Y-%m-%d"),
                        "partition_end":   partition_end.strftime("%Y-%m-%d"),
                        "partition_date":  partition_start.strftime("%Y-%m"),
                        "page":            1,
                    },
                    errback=self.handle_error,
                )

    def parse(self, response):
        """
        Parse one search results page.
        Extract all decision cards, then follow pagination if needed.
        """
        body            = response.meta["body"]
        body_id         = response.meta["body_id"]
        partition_date  = response.meta["partition_date"]
        partition_start = response.meta["partition_start"]
        partition_end   = response.meta["partition_end"]
        page            = response.meta["page"]

        # log partition start as JSON
        self.logger.info(json.dumps({
            "event":          "partition_started",
            "body":           body,
            "partition_date": partition_date,
            "page":           page,
        }))

        cards = response.css("li.each-item.clearfix")

        if not cards:
            self.logger.info(json.dumps({
                "event":          "no_results",
                "body":           body,
                "partition_date": partition_date,
                "page":           page,
            }))
            return

        for card in cards:
            identifier  = card.css("h2.title a::attr(title)").get("").strip()
            title       = card.css("h2.title a::text").get("").strip()
            date_str    = card.css("span.date::text").get("").strip()
            description = card.css("p.description::text").get("").strip()
            ref_no      = card.css("span.refNO::text").get("").strip()
            doc_url     = card.css("a.btn.btn-primary::attr(href)").get("").strip()

            if not identifier:
                continue

            # make relative URLs absolute
            if doc_url and not doc_url.startswith("http"):
                doc_url = f"https://www.workplacerelations.ie{doc_url}"

            yield WplaceItem(
                identifier     = identifier,
                description    = description,
                title          = title,
                ref_no         = ref_no,
                published_date = date_str,
                body           = body,
                doc_url        = doc_url,
                partition_date = partition_date,
                file_path      = None,
                file_hash      = None,
            )

        # ── Pagination ─────────────────────────────────────────────────────────
        next_page = response.css("a.next::attr(href)").get()
        if next_page:
            next_page_num = page + 1
            self.logger.info(json.dumps({
                "event":          "pagination",
                "body":           body,
                "partition_date": partition_date,
                "next_page":      next_page_num,
            }))
            yield response.follow(
                next_page,
                callback=self.parse,
                meta={
                    "body":            body,
                    "body_id":         body_id,
                    "partition_start": partition_start,
                    "partition_end":   partition_end,
                    "partition_date":  partition_date,
                    "page":            next_page_num,
                },
                errback=self.handle_error,
            )

    def handle_error(self, failure):
        self.logger.error(json.dumps({
            "event": "request_failed",
            "url":   failure.request.url,
            "error": str(failure.value),
        }))

    def _build_url(self, from_date, to_date, body_id, page=1):
        """Build the search URL with filters."""
        from_str = from_date.strftime("%-d/%-m/%Y")
        to_str   = to_date.strftime("%-d/%-m/%Y")
        return (
            f"{BASE_URL}"
            f"?decisions=1"
            f"&body={body_id}"
            f"&from={from_str}"
            f"&to={to_str}"
            f"&pageNumber={page}"
        )

    def _generate_monthly_partitions(self):
        """
        Split the full date range into monthly chunks.
        Example: 2024-01-01 to 2024-03-01 →
            (2024-01-01, 2024-01-31)
            (2024-02-01, 2024-02-29)
        """
        partitions = []
        current = self.start_date
        while current < self.end_date:
            partition_end = current + relativedelta(months=1) - timedelta(days=1)
            if partition_end > self.end_date:
                partition_end = self.end_date
            partitions.append((current, partition_end))
            current += relativedelta(months=1)
        return partitions
