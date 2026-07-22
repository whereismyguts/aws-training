import json
import os
import boto3

sns = boto3.client("sns")
SNS_ARN = os.environ["SNS_ARN"]
ALB_DNS = os.environ["ALB_DNS"]


def lambda_handler(event, context):
    for record in event["Records"]:
        meta = json.loads(record["body"])
        text = (
            f"New image uploaded!\n\n"
            f"Name:      {meta['name']}\n"
            f"Size:      {meta['size']} bytes\n"
            f"Extension: {meta['extension']}\n"
            f"Date:      {meta['last_modified']}\n\n"
            f"Download:  http://{ALB_DNS}/images/{meta['name']}"
        )
        sns.publish(
            TopicArn=SNS_ARN,
            Message=text,
            Subject="New image uploaded"
        )
    return {"statusCode": 200}
