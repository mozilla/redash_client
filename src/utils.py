import json
import boto3
from boto.s3.key import Key
from boto3.s3.transfer import S3Transfer

BUCKET = "telemetry-public-analysis-2"
client = boto3.client('s3', 'us-west-2')
transfer = S3Transfer(client)

def upload_as_json(directory_name, filename, data):
  path = "activity-stream/" + directory_name + "/"
  s3_key = path + filename

  json_data = json.dumps(data)
  with open(filename, 'w') as f:
      f.write(json_data)

  transfer.upload_file(filename, BUCKET, s3_key, extra_args={'ContentType':'application/json'})

  return "https://analysis-output.telemetry.mozilla.org/" + s3_key

def download_experiment_definition():
  DIRECTORY_NAME = "experiments/json_definitions"
  FILENAME = "experiments.json"

  path = "activity-stream/" + DIRECTORY_NAME + "/"
  s3_key = path + FILENAME

  experiments_string = transfer.download_file(FILENAME, BUCKET, s3_key)

  try:
    return json.loads(experiments_string)
  except ValueError:
    return {}
