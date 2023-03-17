from search import host, region, service, index_prefix, table_name
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
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

load_dotenv()

credentials = boto3.Session(aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID_2'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY_2']).get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service)
app = Flask(__name__)
CORS(app)

results_per_page = 20

client = OpenSearch(
      hosts = [{'host': host, 'port': 443}],
      http_auth = awsauth,
      use_ssl = True,
      verify_certs = True,
      connection_class = RequestsHttpConnection
    )
db = boto3.client('dynamodb', region_name=region, aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
class Api:
  def __init__(self, name=None):
    self.name = name

  async def query(self, q, from_value=0, start_date="", end_date="", exclude_sides="", exclude_division="", exclude_years="", exclude_schools="", sort_by="", cite_match="", account_id=None, personal_only=""):
    results = self.query_search(q, from_value, start_date, end_date, exclude_sides, exclude_division, exclude_years, exclude_schools, sort_by, cite_match, account_id, personal_only)
    db_results = await asyncio.gather(*[self.get_by_id(result['_id']) for result in results])
    cursor = from_value + len(results)
    return ([result for result in db_results if result != None], cursor)

  def query_search(self, q, from_value, start_date="", end_date="", exclude_sides="", exclude_division="", exclude_years="", exclude_schools="", sort_by="", cite_match="", account_id=None, personal_only=""):
    query = {
      "size": results_per_page,
      "from": from_value,
      "query": {
        "bool": {
          "must": []
        }
      },
      "_source": False
    }

    if q != "":
        q_parts = q.split('\"')

        for part in [part for [i, part] in enumerate(q_parts) if i % 2 == 0]:
          if len(part.strip()) > 0:
            query['query']['bool']['must'] = [{
              "multi_match": {
                "query": part.strip(),
                "fields": ["tag^4", "highlighted_text^3", "cite^3", "body"],
                "fuzziness" : "AUTO",
                "operator":   "and",
                # "analyzer": "syn_analyzer",
                "type": "best_fields",
                "cutoff_frequency": 0.001
              }
            }]
        for part in [part for [i, part] in enumerate(q_parts) if i % 2 == 1]:
          if len(part.strip()) > 0:
            query['query']['bool']['must'].append({
              "multi_match": {
                "query": part,
                "type": "phrase",
                "fields": ["tag^4", "highlighted_text^3", "cite^3", "body"],
                "operator": "and"
              }
            })

    if cite_match != "":
      query['query']['bool']['must'].append({
        "bool": {
          "should": [
            {
              "wildcard": {
                "cite.keyword": "*" + cite_match + "*"
              }
            },
            {
              "wildcard": {
                "cite": "*" + cite_match + "*"
              }
            }
          ]
        }
      })

    if start_date != "" and end_date != "":
      query["query"]["bool"]["filter"] = [
          {
            "range": {
              "cite_date": {
                "gte": start_date,
                "lte": end_date
              }
            }
          }
        ]
    
    if exclude_sides != "":
      query["query"]["bool"]["must_not"] = [{
          "match": {
            "filename": exclude_sides
          }
        }]
    
    if query["query"]["bool"].get("must_not") == None:
        query["query"]["bool"]["must_not"] = []

    if exclude_division != "":
      divisions = exclude_division.split(",")
      for division in divisions:
        query["query"]["bool"]["must_not"].append({
            "match_phrase_prefix": {
              "division": division.split('-')[0]
            }
          })

    if exclude_schools != "":
      schools = exclude_schools.split(",")
      for school in schools:
        query["query"]["bool"]["must_not"].append({
            "term": {
              "school.keyword": school
            }
          })
    
    if exclude_years != "":
      years = exclude_years.split(",")
      for year in years:
        query["query"]["bool"]["must_not"].append({
            "term": {
              "year.keyword": year
            }
          })
      
    index = "personal" if personal_only == 'true' else index_prefix + '*' + (',personal' if account_id is not None else '')

    if account_id != None:
      query["query"]["bool"]["should"] = [
        { "match": { "team": account_id } },
        { "term": { "_index": index }  }
      ]

    print(query)
    print(index)
    response = client.search(
      body=query,
      index=index
    )
    
    return response['hits']['hits']

  def get_colleges(self):
    query = {
      "size": 0,
      "aggs": {
        "schools": {
          "terms": {
            "field": "school.keyword",
            "size": 50000000,
            "order": {
              "_term": "asc",
            }
          }
        }
      }
    }
    response = client.search(
      body=query,
      index=index_prefix + '-college*'
    )
    schools = response['aggregations']['schools']['buckets']
    schools = [school['key'] for school in schools]
    return schools

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
      'display_name': { 'S': user['name'].get('display_name') },
      'first_name': { 'S': user['name'].get('familiar_name') },
      'last_name': { 'S': user['name'].get('surname') },
      'profile_photo_url': { 'S': user.get('profile_photo_url') },
      'country': { 'S': user.get('country') },
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
    job.cancel()

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