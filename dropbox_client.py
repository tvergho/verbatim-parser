import requests
import os

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