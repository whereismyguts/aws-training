import json
import os
import boto3
import pymysql

# DB connection initialized OUTSIDE handler — reused across warm invocations
DB_HOST = os.environ["DB_HOST"]
DB_USER = os.environ["DB_USER"]
DB_PASS = os.environ["DB_PASS"]
DB_NAME = os.environ["DB_NAME"]
S3_BUCKET = os.environ["S3_BUCKET"]

s3 = boto3.client("s3")
db = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS,
                     database=DB_NAME, cursorclass=pymysql.cursors.DictCursor)


def lambda_handler(event, context):
    # Distinguish call source for CloudWatch logs
    source = event.get("detail-type", event.get("source", "web-app"))
    print(f"[{source}] DataConsistency check started")

    # Get all keys from S3
    print(f"[{source}] Listing S3 bucket: {S3_BUCKET}")
    s3_keys = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET):
        for obj in page.get("Contents", []):
            s3_keys.add(obj["Key"])
    print(f"[{source}] S3 done: {len(s3_keys)} objects")

    # Get all names from RDS
    print(f"[{source}] Querying RDS")
    db.ping(reconnect=True)
    with db.cursor() as cur:
        cur.execute("SELECT name FROM images")
        db_names = {row["name"] for row in cur.fetchall()}

    in_s3_not_db = s3_keys - db_names
    in_db_not_s3 = db_names - s3_keys
    consistent = not in_s3_not_db and not in_db_not_s3

    result = {
        "consistent": consistent,
        "s3_count": len(s3_keys),
        "db_count": len(db_names),
        "in_s3_not_db": list(in_s3_not_db),
        "in_db_not_s3": list(in_db_not_s3),
    }
    print(f"[{source}] Result: {json.dumps(result)}")
    return {"statusCode": 200, "body": json.dumps(result)}
