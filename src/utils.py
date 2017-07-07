import json
import boto3
import urllib
from boto3.s3.transfer import S3Transfer

from datetime import datetime, timedelta

BUCKET = "telemetry-public-analysis-2"
client = boto3.client('s3', 'us-west-2')
transfer = S3Transfer(client)
s3 = boto3.client("s3")


def upload_as_json(directory_name, filename, data):
  path = "activity-stream/" + directory_name + "/"
  s3_key = path + filename

  json_data = json.dumps(data)
  file_path = "/tmp/{filename}".format(filename=filename)
  with open(file_path, 'w') as f:
    f.write(json_data)

  transfer.upload_file(
      file_path,
      BUCKET,
      s3_key,
      extra_args={"ContentType": "application/json"})

  return "https://analysis-output.telemetry.mozilla.org/" + s3_key


def read_experiment_definition(url):
  try:
    response = urllib.urlopen(url)
    return json.loads(response.read())
  except:
    return {}


def read_experiment_definition_s3(filename):
  DIRECTORY_NAME = "experiments/json_definitions"

  path = "activity-stream/" + DIRECTORY_NAME + "/"
  s3_key = path + filename

  obj = s3.get_object(Bucket="telemetry-public-analysis-2", Key=s3_key)

  try:
    experiments_string = obj["Body"].read()
    return json.loads(experiments_string)
  except:
    return {}


def format_date(date):
  date_epoch = datetime.fromtimestamp(date / 1000.0)
  date = date_epoch.strftime("%m/%d/%y")
  return date


# A date is considered "old" if it's > 3 days earlier than today.
def is_old_date(date):
  three_old = datetime.today() - timedelta(days=3)
  input_date_epoch = datetime.fromtimestamp(date / 1000.0)
  return input_date_epoch < three_old
