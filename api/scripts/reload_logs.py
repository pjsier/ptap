import json
import os
from datetime import datetime

import boto3
import gspread
from google.oauth2 import service_account


def load_worksheet():
    credentials_json = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT"))
    credentials = service_account.Credentials.from_service_account_info(
        credentials_json,
        scopes=[
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/drive",
        ],
    )

    client = gspread.authorize(credentials)
    return client.open(os.getenv("GOOGLE_SHEET_SUBMISSION_NAME")).worksheet("logs")


def load_logs():
    cloudwatch = boto3.client("logs")
    params = {
        "logGroupName": "/aws/lambda/ptap-prod",
        "startTime": int(datetime(2024, 1, 4).timestamp()) * 1000,
        "limit": 5000,
    }
    log_records = []
    while True:
        res = cloudwatch.filter_log_events(**params)
        print(len(res["events"]))
        if len(res["events"]) > 0:
            print(
                datetime.fromtimestamp(res["events"][0]["timestamp"] / 1000).isoformat()
            )
        for event in res["events"]:
            if "LOG_STEP: " not in event["message"]:
                continue
            message = event["message"].split("LOG_STEP: ")[-1]
            step = json.loads(message)
            if "uuid" in step and step.get("step") == "submit":
                print(
                    datetime.fromtimestamp(event["timestamp"] / 1000).isoformat(),
                    step["uuid"],
                )
                log_records.append(
                    {
                        **step,
                        "timestamp": datetime.fromtimestamp(
                            event["timestamp"] / 1000
                        ).isoformat(),
                    }
                )
        if "nextToken" in res:
            params["nextToken"] = res["nextToken"]
        else:
            break
    return log_records


def row_from_data(data):
    name = data.get("name", f'{data.get("first_name", "")} {data.get("last_name", "")}')
    return [
        data.get("timestamp"),
        data.get("uuid"),
        data.get("step"),
        data.get("region"),
        name,
        data.get("address"),
        data.get("email"),
        data.get("phone"),
        json.dumps(data),
    ]


if __name__ == "__main__":
    worksheet = load_worksheet()
    records = worksheet.get_all_records()
    log_uuids = set([r["uuid"] for r in records])

    log_records = load_logs()
    log_records = sorted(log_records, key=lambda r: r["timestamp"])

    rows_to_add = []
    for rec in log_records:
        if rec["uuid"] not in log_uuids:
            rows_to_add.append(row_from_data(rec))

    worksheet.append_rows(rows_to_add)
