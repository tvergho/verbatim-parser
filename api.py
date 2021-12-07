from search import host, region, service, index_prefix, table_name
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from dynamodb_json import json_util as json
from flask import Flask, request
from flask_cors import CORS
from dotenv import load_dotenv
import boto3
import os
import sys
import asyncio

load_dotenv()

credentials = boto3.Session(aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']).get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service)
app = Flask(__name__)
CORS(app)

results_per_page = 20

class Api:
  def __init__(self):
    self.client = OpenSearch(
      hosts = [{'host': host, 'port': 443}],
      http_auth = awsauth,
      use_ssl = True,
      verify_certs = True,
      connection_class = RequestsHttpConnection
    )
    self.db = boto3.client('dynamodb', region_name=region, aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
  
  async def query(self, q, from_value=0, start_date="", end_date="", exclude_sides="", exclude_division="", exclude_years="", exclude_schools="", sort_by="", cite_match=""):
    results = self.query_search(q, from_value, start_date, end_date, exclude_sides, exclude_division, exclude_years, exclude_schools, sort_by, cite_match)
    db_results = await asyncio.gather(*[self.get_by_id(result['_id']) for result in results])
    cursor = from_value + len(results)
    return ([result for result in db_results if result != None], cursor)

  def query_search(self, q, from_value, start_date="", end_date="", exclude_sides="", exclude_division="", exclude_years="", exclude_schools="", sort_by="", cite_match=""):
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
                "analyzer": "syn_analyzer",
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
      query["query"]["bool"]["must_not"].append({
          "term": {
            "division.keyword": exclude_division
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

    response = self.client.search(
      body=query,
      index=index_prefix + '*'
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
    response = self.client.search(
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
      return self.db.get_item(**kwargs)
    
    response = await loop.run_in_executor(None, get_item)
    
    if response.get('Item') == None:
      return None

    item = json.loads(response['Item'])
    return item

@app.route("/query", methods=['GET'])
def query():
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

  api = Api()
  (results, cursor) = asyncio.run(api.query(search, cursor, 
    start_date=start_date, end_date=end_date, exclude_sides=exclude_sides,
    exclude_division=exclude_division, exclude_schools=exclude_schools, exclude_years=exclude_years, sort_by=sort_by, cite_match=cite_match
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

if __name__ == '__main__':
  app.run(port=os.environ['PORT'], host='0.0.0.0', debug=True)