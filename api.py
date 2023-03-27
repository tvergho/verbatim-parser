from new_search import region, table_name, namespace
from requests.auth import HTTPBasicAuth
from dynamodb_json import json_util as json
from flask import Flask, request, Response
from flask_cors import CORS
from dotenv import load_dotenv
from hashlib import sha256
from worker import registry, conn
from rq.job import Job
from memory_profiler import memory_usage, profile

from dropbox_client import DropboxClient, api_url
import boto3
import os
import hmac
import sys
import traceback
import asyncio
import requests
import threading

import pinecone
import cohere
import logging
import rq_dashboard

load_dotenv()
pinecone.init(api_key=os.environ['PINECONE_KEY'], environment="us-east-1-aws")
index = pinecone.Index("logos")
co = cohere.Client(os.environ['COHERE_KEY'])

app = Flask(__name__)
app.config.from_object(rq_dashboard.default_settings)
app.register_blueprint(rq_dashboard.blueprint, url_prefix="/rq")
CORS(app)

results_per_page = 50

db = boto3.client('dynamodb', region_name=region, aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
logger = logging.getLogger('waitress')

class Api:
  def __init__(self, name=None):
    self.name = name

  async def query(self, q, from_value=0, start_date="", end_date="", exclude_sides="", exclude_division="", exclude_years="", exclude_schools="", sort_by="", cite_match="", account_id=None, personal_only=""):
    results = self.query_search(q, from_value, start_date, end_date, exclude_sides, exclude_division, exclude_years, exclude_schools, sort_by, cite_match, account_id, personal_only)
    db_results = await asyncio.gather(*[self.get_by_id(result) for result in results[from_value:from_value+results_per_page]])
    cursor = from_value + results_per_page
    return ([result for result in db_results if result != None], cursor)

  def query_search(self, q, from_value, start_date="", end_date="", exclude_sides="", exclude_division="", exclude_years="", exclude_schools="", sort_by="", cite_match="", account_id=None, personal_only=""):
    cohere_response = co.embed(
      texts=[q], 
      truncate="END"
    )
    embeddings = cohere_response.embeddings

    filter_dict = {'$and': [], '$or': []}

    if start_date != "" and end_date != "":
      filter_dict['$and'].append({
        "cite_date": {
          "$gte": int(start_date),
          "$lte": int(end_date)
        }
      })
    if exclude_division != "":
      for division in exclude_division.split(","):
        filter_dict['$and'].append({
          "division": {
            "$ne": division
          }
        })
    if exclude_schools != "":
      for school in exclude_schools.split(","):
        filter_dict['$and'].append({
          "school": {
            "$ne": school
          }
        })
    if exclude_years != "":
      for year in exclude_years.split(","):
        filter_dict['$and'].append({
          "year": {
            "$ne": year
          }
        })

    if personal_only == 'true':
      filter_dict['$and'].append({
        "division": {
          "$eq": "personal"
        }
      })
    else:
      filter_dict['$or'].extend([{
        "division": {
          "$eq": "ndtceda"
        }
      }, {
        "division": {
          "$eq": "hspolicy"
        }
      }])

    if account_id != None:
      filter_dict['$or'].append({
        "team": {
          "$eq": account_id
        }
      })

    response = index.query(
      namespace=namespace,
      top_k=200,
      include_values=False,
      include_metadata=False,
      vector=embeddings[0],
      filter=filter_dict
    )

    # if cite_match != "": 

    # if exclude_sides != "":
      
    # index = "personal" if personal_only == 'true' else index_prefix + '*' + (',personal' if account_id is not None else '')

    # if account_id != None:
    
    matches = list(map(lambda x : x['id'], response.matches))
    return matches

  def get_colleges(self):
    return ['AmherstHarvard', 'ArizonaState', 'ArizonaStateUniversity', 'Army', 'Baylor', 'Binghamton', 'CSUFullerton', 'CSULongBeach', 'CSUNorthridge', 'CalBerkeley', 'CalStateFullerton', 'CentralOklahoma', 'Columbia', 'Cornell', 
    'Dartmouth', 'Emory', 'EmoryUniversity', 'EmporiaState', 'FresnoCityCollege', 'FullertonCollege', 'GeorgeMason', 'Georgetown', 'Georgia', 'GeorgiaState', 'Gonzaga', 
    'Harvard', 'Houston', 'Indiana', 'Iowa', 'JCCC', 'JamesMadison', 'KCKCC', 'Kansas', 'KansasState', 'Kentucky', 'Liberty', 'LibertyUniversity', 'Louisville', 'Macalester', 
    'MaryWashington', 'Michigan', 'MichiganState', 'MichiganStateUniversity', 'Minnesota', 'Minnesota-Houston', 'Minnesota-INTEC', 'Minnesota-Indiana', 'MissouriState', 'Monmouth', 'NYU', 
    'Navy', 'NebraskaLincoln', 'NewSchool', 'Northeastern', 'Northwestern', 'Oakton', 'Oklahoma', 'PennStateDebate', 'Pennsylvania', 'Pittsburgh', 'Purdue', 'Rochester', 'Rutgers', 'SaintMarys', 'Samford', 
    'SouthernCalifornia', 'SouthernNazarene', 'SouthwesternCollege', 'Texas', 'TexasTech', 'Towson', 'Trinity', 'Tufts', 'TuftsUniversity', 'UMassAmherst', 'UNLV', 'UTD', 'UTSA', 'UTSanAntonio', 'WKU', 'WVU', 'WakeForest', 
    'WakeForestUniversity', 'Washington', 'WashingtonUniversity', 'WayneState', 'WeberState', 'WeberStateUniversity', 'WestGeorgia', 'WestVirginiaUniversity', 'WesternWashington', 'WesternWashingtonUniversity', 'WichitaState', 'Wyoming']

  async def get_by_id(self, id, preview=True):
    loop = asyncio.get_event_loop()

    def get_item():
      kwargs = {
        'TableName': table_name,
        'Key': {
          'id': {
            'S': id
          }
        },
        'ReturnConsumedCapacity': 'NONE'
      }
      if preview == True:
        kwargs['ProjectionExpression'] = "id,title,cite,tag,division,#y,s3_url,download_url,cite_emphasis"
        kwargs['ExpressionAttributeNames'] = {
          '#y': 'year'
        }
      return db.get_item(**kwargs)
    
    response = await loop.run_in_executor(None, get_item)
    
    if response.get('Item') == None:
      return None

    item = json.loads(response['Item'])
    return item
  
  def create_or_update_user(self, account_id, user, refresh_token):
    user = {
      'account_id': { 'S': account_id },
      'email': { 'S': user.get('email', '') },
      'display_name': { 'S': user['name'].get('display_name', '') },
      'first_name': { 'S': user['name'].get('familiar_name', '') },
      'last_name': { 'S': user['name'].get('surname', '') },
      'profile_photo_url': { 'S': user.get('profile_photo_url', '') },
      'country': { 'S': user.get('country', '') },
      'refresh_token': { 'S': refresh_token }
    }

    db.put_item(
      TableName="logos-users",
      Item=user
    )
  
  def get_access_for_user(self, account_id):
    kwargs = {
      'TableName': "logos-users",
      'Key': {
        'account_id': {
          'S': account_id
        }
      },
      'ReturnConsumedCapacity': 'NONE'
    }
    response = db.get_item(**kwargs)

    if response.get('Item') == None:
      return None
    print(response['Item'])
    refresh_token = response['Item']['refresh_token']['S']
    files = response['Item'].get('files', {})

    refreshed_token = requests.post('https://api.dropboxapi.com/oauth2/token', headers={ 'content-type': 'application/x-www-form-urlencoded' }, data={
      'refresh_token': refresh_token,
      'grant_type': 'refresh_token'
    }, auth=HTTPBasicAuth(os.environ.get('DROPBOX_CLIENT_ID'), os.environ.get('DROPBOX_CLIENT_SECRET')))

    if refreshed_token.status_code != 200:
      return None
    
    return refreshed_token.json()['access_token'], files

def check_auth(token):
  try:
    dropbox = DropboxClient(token)
    account_info = dropbox.get_user_info()
    print(account_info)
    return account_info
  except Exception as e:
    print(e)
    traceback.format_exc()
    return False

@app.route("/query", methods=['GET'])
def query():
  access_token = request.args.get('access_token')
  account_id = None

  if access_token != None:
    info = check_auth(access_token)
    if info == False or info.get('account_id') == None:
      return { 'error': 'Invalid access token' }, 401
    account_id = info['account_id'].split(':')[1]

  search = request.args.get('search')
  cursor = int(request.args.get('cursor', 0))
  start_date = request.args.get('start_date', '')
  end_date = request.args.get('end_date', '')
  exclude_sides = request.args.get('exclude_sides', '')
  exclude_division = request.args.get('exclude_division', '')
  exclude_schools = request.args.get('exclude_schools', '')
  exclude_years = request.args.get('exclude_years', '')
  sort_by = request.args.get('sort_by', '')
  cite_match = request.args.get('cite_match', '')
  personal_only = request.args.get('personal_only', '')

  if not account_id and personal_only == 'true':
    return { 'error': 'You must be logged in to view your personal files' }, 401

  api = Api()
  (results, cursor) = asyncio.run(api.query(search, cursor, 
    start_date=start_date, end_date=end_date, exclude_sides=exclude_sides,
    exclude_division=exclude_division, exclude_schools=exclude_schools, exclude_years=exclude_years, sort_by=sort_by, cite_match=cite_match,
    account_id=account_id, personal_only=personal_only
  ))
  return {"count": len(results), "results": results, "cursor": cursor}

@app.route("/card", methods=['GET'])
def get_card():
  card_id = request.args.get('id')
  api = Api()
  result = asyncio.run(api.get_by_id(card_id, False))
  return result

@app.route("/schools", methods=['GET'])
def get_schools_list():
  api = Api()
  schools = api.get_colleges()
  return {"colleges": schools}

@app.route("/create-user", methods=['POST'])
def create_user():
  try:
    access_token = request.headers.get('Authorization')

    if access_token == None:
      return { 'error': 'No access token provided' }, 401
    if request.json == None or request.json.get('refresh_token') == None:
      return { 'error': 'No refresh token provided' }, 400

    refresh_token = request.json['refresh_token']

    print(access_token)
    dropbox = DropboxClient(access_token)
    api = Api()

    account_info = dropbox.get_user_info()
    api.create_or_update_user(account_info['account_id'], account_info, refresh_token)
    return account_info
  except Exception as e:
    traceback.print_exc()
    return {"error": str(e)}, 400

def cancel_user_jobs(account_id):
  running_job_ids = registry.get_job_ids()
  user_jobs = [job for job in running_job_ids if job.startswith(account_id)]
  for job_id in user_jobs:
    job = Job.fetch(job_id, connection=conn)
    job.delete()

def process_user(account_id):
  print(account_id)
  api = Api()
  access_token, files = api.get_access_for_user(account_id)
  print(access_token)
  dropbox = DropboxClient(access_token)
  files = dropbox.get_all_files()
  cancel_user_jobs(account_id)
  dropbox.process_files(files, account_id)

@app.route('/webhook', methods=['GET', 'POST'])
def verify():
  if request.method == 'GET':
    resp = Response(request.args.get('challenge'))
    resp.headers['Content-Type'] = 'text/plain'
    resp.headers['X-Content-Type-Options'] = 'nosniff'

    return resp

  try:
    signature = request.headers.get('X-Dropbox-Signature')
    key = bytes(os.environ['DROPBOX_CLIENT_SECRET'], encoding="ascii")
    computed_signature = hmac.new(key, request.data, sha256).hexdigest()
    if not hmac.compare_digest(signature, computed_signature):
      abort(403)

    logger.info(f"Webhook received: {request.json}")
    if request.json.get('list_folder') == None or request.json['list_folder'].get('accounts') == None:
      return "empty accounts"
    
    for account in request.json['list_folder']['accounts']:
      process_user(account)
    
    return 'OK'
  except Exception as e:
    traceback.print_exc()
    print(e)
    return {"error": str(e)}, 400

if __name__ == '__main__':
  app.run(port=os.environ['PORT'], host='0.0.0.0', debug=True)