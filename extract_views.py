# %%
import datetime
import json
from pathlib import Path

import boto3
import requests


def query_wiki(url):
    wiki_server_response = requests.get(url, headers={"User-Agent": "curl/7.68.0"})
    wiki_response_status = wiki_server_response.status_code
    if wiki_response_status != 200:
        raise ValueError(f"Failed to get data from {url}")

    return wiki_server_response


def get_top_views(DATE_PARAM, date_, dir_):
    # URL constructed based on https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/reference/page-views.html#list-most-viewed-pages
    api = "https://wikimedia.org/api/rest_v1/metrics"
    suburl = f"/pageviews/top/en.wikipedia.org/all-access/{date_.strftime('%Y/%m/%d')}"
    url = api + suburl

    wiki_server_response = query_wiki(url)
    wiki_response_body = wiki_server_response.text

    RAW_LOCATION_BASE = dir_ / "data" / "raw-views"
    RAW_LOCATION_BASE.mkdir(exist_ok=True, parents=True)

    filename = "raw-views-" + DATE_PARAM + ".txt"
    raw_views_file = RAW_LOCATION_BASE / filename
    with open(raw_views_file, "w") as f:
        f.write(wiki_response_body)

    return raw_views_file, filename, wiki_server_response


def s3_upload_views(s3_client, S3_WIKI_BUCKET, raw_views_file, filename):
    s3_key = "datalake/raw/" + filename
    s3_client.upload_file(str(raw_views_file), S3_WIKI_BUCKET, s3_key)
    return None


def process_views(s3_client, wiki_server_response, date_, dir_):
    wiki_response_parsed = wiki_server_response.json()
    top_views = wiki_response_parsed["items"][0]["articles"]

    current_time = datetime.datetime.now(datetime.timezone.utc)
    json_lines = ""
    for page in top_views:
        record = {
            "article": page["article"],
            "views": page["views"],
            "rank": page["rank"],
            "date": date_.strftime("%Y-%m-%d"),
            "retrieved_at": current_time.replace(
                tzinfo=None
            ).isoformat(),  # We need to remove tzinfo as Athena cannot work with offsets
        }
        json_lines += json.dumps(record) + "\n"

    JSON_LOCATION_DIR = dir_ / "data" / "views"
    JSON_LOCATION_DIR.mkdir(exist_ok=True, parents=True)

    json_lines_filename = f"views-{date_.strftime('%Y-%m-%d')}.json"
    json_lines_file = JSON_LOCATION_DIR / json_lines_filename

    with json_lines_file.open("w") as file:
        file.write(json_lines)

    return json_lines_file, json_lines_filename


def __main__():
    s3 = boto3.client(
        "s3",
        region_name="eu-west-1",  # Ireland region
    )
    S3_WIKI_BUCKET = "de2-mihaly-wikidata"
    current_directory = Path(__file__).parent
    for DATE_PARAM in ["2024-11-15", "2024-11-16", "2024-11-17", "2024-11-18"]:
        date_ = datetime.datetime.strptime(DATE_PARAM, "%Y-%m-%d")
        raw_views_file, filename, wiki_server_response = get_top_views(DATE_PARAM, date_, current_directory)
        s3_upload_views(s3, S3_WIKI_BUCKET, raw_views_file, filename)
        json_lines_file, json_lines_filename = process_views(s3, wiki_server_response, date_, current_directory)
        s3.upload_file(json_lines_file, S3_WIKI_BUCKET, f"datalake/views/{json_lines_filename}")
    print("Finished processing views")


if __name__ == "__main__":
    __main__()
