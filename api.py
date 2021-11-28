from search import host, region, service, index_prefix, table_name
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from dynamodb_json import json_util as json
from flask import Flask, request
from flask_cors import CORS
import boto3
import sys
import asyncio

credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service)
app = Flask(__name__)
CORS(app)

results_per_page = 100

class Api:
  def __init__(self):
    self.client = OpenSearch(
      hosts = [{'host': host, 'port': 443}],
      http_auth = awsauth,
      use_ssl = True,
      verify_certs = True,
      connection_class = RequestsHttpConnection
    )
    self.db = boto3.client('dynamodb', region_name=region)
  
  async def query(self, q):
    results = self.query_search(q)
    db_results = await asyncio.gather(*[self.get_by_id(result['_id']) for result in results])
    return [result for result in db_results if result != None]

  def query_search(self, q):
    query = {
      "size": results_per_page,
      "query": {
        "multi_match": {
          "query": q,
          "fields": ["tag^4", "cite^2", "body"]
        }
      },
      "_source": False
    }

    response = self.client.search(
      body=query,
      index=index_prefix + '*'
    )

    return response['hits']['hits']

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
        kwargs['ProjectionExpression'] = "id,title,cite,tag,division,#y,s3_url"
        kwargs['ExpressionAttributeNames'] = {
          '#y': 'year'
        }
      return self.db.get_item(**kwargs)
    
    response = await loop.run_in_executor(None, get_item)
    
    if response.get('Item') == None:
      return None

    item = json.loads(response['Item'])
    return item

@app.route("/query", methods=['GET'])
def query():
  search = request.args.get('search')
  api = Api()
  results = asyncio.run(api.query(search))
  return {"count": len(results), "results": results}

@app.route("/card", methods=['GET'])
def get_card():
  card_id = request.args.get('id')
  api = Api()
  result = asyncio.run(api.get_by_id(card_id, False))
  return result

if __name__ == '__main__':
  if len(sys.argv) != 2:
    print("Usage: python3 api.py <query>")
    sys.exit(1)

  query = sys.argv[1]

  api = Api()
  response = api.query(query)

  print(response)