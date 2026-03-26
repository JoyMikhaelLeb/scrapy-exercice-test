import subprocess
import json
import logging
from dagster import op, job, In, Nothing, get_dagster_logger
import os


# ── Ops ────────────────────────────────────────────────────────────────────────

@op
def scrape_op(context):
    """
    Runs the Scrapy spider.
    Accepts start_date and end_date from the job config.
    """
    start_date = context.op_config["start_date"]
    end_date   = context.op_config["end_date"]
    body       = context.op_config.get("body", None)

    logger = get_dagster_logger()
    logger.info(json.dumps({
        "event":      "scrape_started",
        "start_date": start_date,
        "end_date":   end_date,
        "body":       body,
    }))

    cmd = [
        "python", "-m", "scrapy", "crawl", "wplace",
        "-a", f"start_date={start_date}",
        "-a", f"end_date={end_date}",
    ]

    if body:
        cmd += ["-a", f"body={body}"]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    )

    if result.returncode != 0:
        logger.error(json.dumps({
            "event": "scrape_failed",
            "error": result.stderr,
        }))
        raise Exception(f"Scrapy failed:\n{result.stderr}")

    logger.info(json.dumps({
        "event":  "scrape_finished",
        "output": result.stdout[-500:],  # last 500 chars of output
    }))


@op(ins={"after_scrape": In(Nothing)})
def transform_op(context):
    """
    Runs the transformation script.
    Only runs after scrape_op finishes successfully.
    """
    start_date = context.op_config["start_date"]
    end_date   = context.op_config["end_date"]

    logger = get_dagster_logger()
    logger.info(json.dumps({
        "event":      "transform_started",
        "start_date": start_date,
        "end_date":   end_date,
    }))

    result = subprocess.run(
        [
            "python", "transformation/transform.py",
            "--start-date", start_date,
            "--end-date",   end_date,
        ],
        capture_output=True,
        text=True,
        cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    )

    if result.returncode != 0:
        logger.error(json.dumps({
            "event": "transform_failed",
            "error": result.stderr,
        }))
        raise Exception(f"Transform failed:\n{result.stderr}")

    logger.info(json.dumps({
        "event":  "transform_finished",
        "output": result.stdout[-500:],
    }))


# ── Job ────────────────────────────────────────────────────────────────────────

@job
def wplace_pipeline():
    """
    Full pipeline:
    1. scrape_op  → scrapes WRC website
    2. transform_op → cleans and transforms the scraped files
    transform_op only runs if scrape_op succeeds.
    """
    transform_op(after_scrape=scrape_op())