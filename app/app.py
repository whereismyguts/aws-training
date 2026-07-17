import requests
from flask import Flask

app = Flask(__name__)

def get_metadata(path):
    token = requests.put(
        "http://169.254.169.254/latest/api/token",
        headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
        timeout=2
    ).text
    return requests.get(
        f"http://169.254.169.254/latest/meta-data/{path}",
        headers={"X-aws-ec2-metadata-token": token},
        timeout=2
    ).text

@app.route("/")
def index():
    region = get_metadata("placement/region")
    az = get_metadata("placement/availability-zone")
    return f"<h1>Region: {region}</h1><h2>AZ: {az}</h2>"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80)
