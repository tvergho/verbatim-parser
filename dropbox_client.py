import requests
import os
import json
import asyncio
import boto3

from parser import Parser
from search import region, Search

api_url = "https://api.dropbox.com"
s3_url = "https://logos-debate-2.s3.us-west-1.amazonaws.com"
bucket = "logos-debate-2"

search = Search()
class DropboxClient:
  def __init__(self, access_token):
    self.client_id = os.environ.get('DROPBOX_CLIENT_ID')
    self.client_secret = os.environ.get('DROPBOX_CLIENT_SECRET')
    self.access_token = access_token if 'Bearer' in access_token else f"Bearer {access_token}"
    self.headers = {
      "Authorization": self.access_token,
    }
    self.s3 = boto3.client('s3', region_name=region, aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])


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
  

  async def process_file(self, dropbox_file, account_id):
    path = dropbox_file['id']
    filename = dropbox_file['name']
    path_lower = dropbox_file['path_lower']

    if path_lower.split('.')[len(path_lower.split('.')) - 1] != 'docx':
      return

    response = requests.post('https://content.dropboxapi.com/2/files/download', headers={
      **self.headers,
      'Dropbox-API-Arg': json.dumps({ "path": path })
    })

    tmp_name = f'{dropbox_file["content_hash"]}.docx'
    tmp_path = f'/tmp/{tmp_name}'
    trunc_account_id = account_id.split(':')[1]

    print(f'processing {tmp_path}')

    with open(tmp_path, 'wb') as f:
      for chunk in response.iter_content(1024 * 1024 * 2):
        f.write(chunk)

    s3_response = self.s3.upload_file(tmp_path, bucket, f"{trunc_account_id}/{filename}", ExtraArgs={'ACL':'public-read'})

    additional_info = {
      "filename": filename,
      "school": "",
      "team": trunc_account_id,
      "division": "personal",
      "year": "",
      "download_url": f"{s3_url}/{trunc_account_id}/{filename}"
    }
    parser = Parser(tmp_path, additional_info)
    cards = parser.parse()

    search.upload_cards(cards, opt_prefix=f"cards2-personal")
    search.upload_to_dynamo(cards)

    print(f'processed {filename}')
    os.remove(tmp_path)

  async def process_files(self, files, account_id):
    await asyncio.gather(*[self.process_file(dropbox_file, account_id) for dropbox_file in files])
      