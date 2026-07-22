import json
import os
import random
import boto3
import pymysql
import requests
from flask import Flask, request, jsonify, send_file
from io import BytesIO

app = Flask(__name__)

S3_BUCKET  = os.environ.get("S3_BUCKET", "anton-karmanov-website")
DB_HOST    = os.environ.get("DB_HOST", "localhost")
DB_USER    = os.environ.get("DB_USER", "admin")
DB_PASS    = os.environ.get("DB_PASS", "")
DB_NAME    = os.environ.get("DB_NAME", "cloudx")
SQS_URL    = os.environ.get("SQS_URL", "")
SNS_ARN              = os.environ.get("SNS_ARN", "")
ALB_DNS              = os.environ.get("ALB_DNS", "")
AWS_REGION           = os.environ.get("AWS_REGION", "eu-north-1")
CONSISTENCY_FUNC     = os.environ.get("CONSISTENCY_FUNC", "CloudX-DataConsistencyFunction")

s3      = boto3.client("s3",     region_name=AWS_REGION)
sqs     = boto3.client("sqs",    region_name=AWS_REGION)
sns     = boto3.client("sns",    region_name=AWS_REGION)
lmb     = boto3.client("lambda", region_name=AWS_REGION)


# ─── DB ───────────────────────────────────────────────────────────────────────

def get_db():
    return pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS,
                           database=DB_NAME, cursorclass=pymysql.cursors.DictCursor)

def init_db():
    db = get_db()
    with db.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS images (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) UNIQUE NOT NULL,
                size INT NOT NULL,
                extension VARCHAR(32),
                last_modified DATETIME NOT NULL
            )
        """)
    db.commit()
    db.close()

def db_upsert(meta):
    db = get_db()
    with db.cursor() as cur:
        cur.execute("""
            INSERT INTO images (name, size, extension, last_modified)
            VALUES (%(name)s, %(size)s, %(extension)s, %(last_modified)s)
            ON DUPLICATE KEY UPDATE
              size=VALUES(size), extension=VALUES(extension), last_modified=VALUES(last_modified)
        """, meta)
    db.commit()
    db.close()

def db_delete(name):
    db = get_db()
    with db.cursor() as cur:
        cur.execute("DELETE FROM images WHERE name=%s", (name,))
    db.commit()
    db.close()

def db_get(name):
    db = get_db()
    with db.cursor() as cur:
        cur.execute("SELECT * FROM images WHERE name=%s", (name,))
        row = cur.fetchone()
    db.close()
    return row

def db_random():
    db = get_db()
    with db.cursor() as cur:
        cur.execute("SELECT * FROM images ORDER BY RAND() LIMIT 1")
        row = cur.fetchone()
    db.close()
    return row


# ─── EC2 metadata ─────────────────────────────────────────────────────────────

def get_metadata(path):
    token = requests.put(
        "http://169.254.169.254/latest/api/token",
        headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"}, timeout=2
    ).text
    return requests.get(
        f"http://169.254.169.254/latest/meta-data/{path}",
        headers={"X-aws-ec2-metadata-token": token}, timeout=2
    ).text


# ─── S3 helpers ───────────────────────────────────────────────────────────────

def s3_meta(key):
    head = s3.head_object(Bucket=S3_BUCKET, Key=key)
    return {
        "name": key,
        "size": head["ContentLength"],
        "extension": os.path.splitext(key)[1],
        "last_modified": head["LastModified"].isoformat(),
    }


# ─── SQS/SNS helpers ──────────────────────────────────────────────────────────

def send_to_queue(meta):
    if not SQS_URL:
        return
    sqs.send_message(
        QueueUrl=SQS_URL,
        MessageBody=json.dumps(meta),
        DelaySeconds=5
    )

# Queue processing moved to CloudX-UploadsNotificationFunction Lambda


# ─── Routes ───────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    region = get_metadata("placement/region")
    az = get_metadata("placement/availability-zone")
    return f"<h1>Region: {region}</h1><h2>AZ: {az}</h2>"


@app.route("/images", methods=["POST"])
def upload():
    f = request.files.get("file")
    if not f:
        return jsonify(error="no file"), 400
    s3.upload_fileobj(f, S3_BUCKET, f.filename)
    meta = s3_meta(f.filename)
    db_upsert(meta)
    send_to_queue(meta)          # → SQS → worker → SNS → email
    return jsonify(meta), 201


@app.route("/images/<name>", methods=["GET"])
def download(name):
    obj = s3.get_object(Bucket=S3_BUCKET, Key=name)
    return send_file(BytesIO(obj["Body"].read()), download_name=name, as_attachment=True)


@app.route("/images/<name>/metadata", methods=["GET"])
def metadata(name):
    row = db_get(name)
    if not row:
        return jsonify(error="not found"), 404
    return jsonify(row)


@app.route("/images/random/metadata", methods=["GET"])
def random_meta():
    row = db_random()
    if not row:
        return jsonify(error="no images"), 404
    return jsonify(row)


@app.route("/images/<name>", methods=["DELETE"])
def delete(name):
    s3.delete_object(Bucket=S3_BUCKET, Key=name)
    db_delete(name)
    return jsonify(deleted=name)


@app.route("/consistency", methods=["GET"])
def consistency():
    resp = lmb.invoke(
        FunctionName=CONSISTENCY_FUNC,
        InvocationType="RequestResponse",
        Payload=json.dumps({"source": "web-app", "detail-type": "web-app"})
    )
    result = json.loads(resp["Payload"].read())
    return jsonify(json.loads(result["body"]))


@app.route("/notifications/subscriptions/<email>", methods=["POST"])
def subscribe(email):
    if not SNS_ARN:
        return jsonify(error="SNS not configured"), 500
    sns.subscribe(TopicArn=SNS_ARN, Protocol="email", Endpoint=email)
    return jsonify(message=f"Confirmation email sent to {email}"), 200


@app.route("/notifications/subscriptions/<email>", methods=["DELETE"])
def unsubscribe(email):
    if not SNS_ARN:
        return jsonify(error="SNS not configured"), 500
    subs = sns.list_subscriptions_by_topic(TopicArn=SNS_ARN)["Subscriptions"]
    for sub in subs:
        if sub["Endpoint"] == email:
            sns.unsubscribe(SubscriptionArn=sub["SubscriptionArn"])
    return jsonify(message=f"Unsubscribed {email}"), 200


if __name__ == "__main__":
    try:
        init_db()
    except Exception as e:
        print(f"DB init failed: {e}")
    app.run(host="0.0.0.0", port=80)
