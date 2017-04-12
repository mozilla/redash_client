import json
import boto3
from boto3.s3.transfer import S3Transfer

bucket = "telemetry-public-analysis-2"
client = boto3.client('s3', 'us-west-2')
transfer = S3Transfer(client)

#filename = "test.json"

def upload_as_json(directory_name, filename, data):
  path = "activity-stream/" + directory_name + "/"
  s3_key = path + filename

  json_data = json.dumps(data)
  with open(filename, 'w') as f:
      f.write(json_data)

  transfer.upload_file(filename, bucket, s3_key, extra_args={'ContentType':'application/json'})

  return "https://analysis-output.telemetry.mozilla.org/" + s3_key
