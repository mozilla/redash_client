import os
import sys
import pickle
import httplib2
import argparse
import apiclient
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage
from apiclient.discovery import build

class SheetsClient(object):
  # If modifying these scopes, delete your previously saved credentials
  # at ~/.credentials/sheets.googleapis.com-python.json
  SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
  CLIENT_SECRET_FILE = 'client_secret.json'

  def __init__(self):
    self._service = self._get_service()

  def get_credentials(self):
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
      Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')

    if not os.path.exists(credential_dir):
      os.makedirs(credential_dir)

    credential_path = os.path.join(credential_dir, 'sheets.googleapis.com-python.json')
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
    store = Storage(credential_path)
    credentials = store.get()

    if not credentials or credentials.invalid:
      flow = client.flow_from_clientsecrets(self.CLIENT_SECRET_FILE, self. SCOPES)
      credentials = tools.run_flow(flow, store, flags)
      print('Storing credentials to ' + credential_path)

    return credentials

  def _add_permissions(self, file_id, email):
    credentials = self.get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = build('drive', 'v3', http=http)
    service.permissions().create(
      fileId=file_id,
      body = {
      "emailAddress": email,
      "type": "user",
      "role": "writer"
    }).execute()

  def _get_service(self):
    credentials = self.get_credentials()
    http = credentials.authorize(httplib2.Http())
    discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?version=v4')
    return discovery.build('sheets', 'v4', http=http,
                 discoveryServiceUrl=discoveryUrl)

  def create_sheet(self, title, gservice_email):
    # If we don't have a sheets file, create one.
    saved_sheets = {}
    if not os.path.isfile("sheets.pickle"):
      pickle.dump(saved_sheets, open("sheets.pickle", "wb"))
    saved_sheets = pickle.load(open("sheets.pickle", "rb"))

    if title not in saved_sheets:
      data = {'properties': {'title': title}}
      result = self._service.spreadsheets().create(body=data).execute()
      saved_sheets[title] = result["spreadsheetId"]
      pickle.dump(saved_sheets, open("sheets.pickle", "wb"))

    file_id = saved_sheets[title]
    self._add_permissions(file_id, gservice_email)

    return file_id

  def write_to_sheet(self, title, values, gservice_email):
    for i in xrange(3):
      try:
        saved_sheets = pickle.load(open("sheets.pickle", "rb"))
        spreadsheet_id = saved_sheets[title] if title in saved_sheets else self.create_sheet(title, gservice_email)
        self._service.spreadsheets().values().update(
          spreadsheetId=spreadsheet_id, range="Sheet1",
          valueInputOption="USER_ENTERED", body={'values': values}).execute()
        return spreadsheet_id
      except:
        # The update could fail if the sheet in the pickled file no longer exists
        if title in saved_sheets:
          del saved_sheets[title]
        pickle.dump(saved_sheets, open("sheets.pickle", "wb"))
        self.create_sheet(title, gservice_email)
