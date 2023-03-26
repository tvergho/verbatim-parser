import requests
import os
import json
import asyncio
import boto3
from rq.job import Job

from local_parser import Parser
from new_search import region, Search
from memory_profiler import memory_usage, profile
from worker import conn, q
import logging

api_url = "https://api.dropbox.com"
s3_url = "https://logos-debate-2.s3.us-west-1.amazonaws.com"
bucket = "logos-debate-2"

search = Search()
s3 = boto3.client('s3', region_name=region, aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
db = boto3.client('dynamodb', region_name=region, aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])

logger = logging.getLogger('waitress')

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
  
  # @profile
  def process_file(self, dropbox_file, account_id):
    trunc_account_id = account_id.split(':')[1]

    path = dropbox_file['id']
    name = dropbox_file['name']
    path_lower = dropbox_file['path_lower']
    path_display = dropbox_file['path_display']
    content_hash = dropbox_file['content_hash']

    filename = f"{trunc_account_id}{path_display}"

    if path_lower.split('.')[len(path_lower.split('.')) - 1] != 'docx':
      return
    
    if search.check_content_hash_in_dynamo(account_id, content_hash):
      print(f"Skipping {filename} because it already exists in search")
      return

    response = requests.post('https://content.dropboxapi.com/2/files/download', headers={
      **self.headers,
      'Dropbox-API-Arg': json.dumps({ "path": path })
    })

    tmp_name = f'{dropbox_file["content_hash"]}.docx'
    tmp_path = f'/tmp/{tmp_name}'

    print(f'processing {tmp_path}, {name}')

    with open(tmp_path, 'wb') as f:
      for chunk in response.iter_content(1024 * 1024 * 2):
        f.write(chunk)

    s3_response = s3.upload_file(tmp_path, bucket, filename, ExtraArgs={'ACL':'public-read'})
    print(f'uploaded {tmp_path} to {filename}')

    additional_info = {
      "filename": filename,
      "school": "",
      "team": trunc_account_id,
      "division": "personal",
      "year": "",
      "download_url": f"{s3_url}/{filename}",
      "content_hash": dropbox_file['content_hash'],
    }
    parser = Parser(tmp_path, additional_info)
    cards = parser.parse()

    print(f'parsed {len(cards)} cards from {tmp_path}')
    search.upload_cards(cards)
    search.upload_to_dynamo(cards)
    search.upload_all_remaining_cards()

    print(f'processed {filename}')
    try:
      os.remove(tmp_path)
    except:
      print(f'Could not remove {tmp_path}')

  # @profile
  def process_files(self, files, account_id):
    for dropbox_file in files:
      if dropbox_file['.tag'] == 'file':
        job_id = f"{account_id}-{dropbox_file['content_hash']}"
        if not Job.exists(job_id, connection=conn):
          q.enqueue(self.process_file, dropbox_file, account_id, job_id=job_id, failure_ttl=60)
        else:
          logger.info(f"Skipping {job_id} because it already exists in the queue")
    
    # Remove files that are no longer in dropbox
    print(f"Removing {len(files)} files that are no longer in dropbox")
    trunc_account_id = account_id.split(':')[1]
    search.remove_files(files, trunc_account_id)