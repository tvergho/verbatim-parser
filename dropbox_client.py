import requests
import os
import json

api_url = "https://api.dropbox.com"

class DropboxClient:
  def __init__(self, access_token):
    self.client_id = os.environ.get('DROPBOX_CLIENT_ID')
    self.client_secret = os.environ.get('DROPBOX_CLIENT_SECRET')
    self.access_token = access_token if 'Bearer' in access_token else f"Bearer {access_token}"
    self.headers = {
      "Authorization": self.access_token,
    }

  def get_user_info(self):
    response = requests.post(f'{api_url}/2/users/get_current_account', headers=self.headers, json=None)
    return response.json()
  
  def get_all_files(self):
    response = requests.post(f'{api_url}/2/files/list_folder', headers=self.headers, json={
      'path': '',
      'recursive': True
    })
    data = response.json()
    entries = data['entries']

    while data['has_more'] == True:
      response = requests.post(f'{api_url}/2/files/list_folder/continue', headers=self.headers, json={
        'cursor': data['cursor']
      })
      data = response.json()
      entries += data['entries']

    return entries
  
  def process_files(self, files):
    for file in files:
      path = file['id']
      response = requests.post('https://content.dropboxapi.com/2/files/download', headers={
        **self.headers,
        'Dropbox-API-Arg': json.dumps({ "path": path })
      })

      with open(f'/tmp/{file["content_hash"]}.docx', 'wb') as f:
        for chunk in response.iter_content(1024 * 1024 * 2):
          f.write(chunk)