from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from dotenv import load_dotenv
import os
import boto3
import json

load_dotenv()

host = os.environ['AWS_OPENSEARCH_HOST']
region = 'us-west-1'
table_name = 'logos-debate'
index_prefix = "cards3"

# service = 'es'
# credentials = boto3.Session(aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']).get_credentials()
# awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service)
class Search():
  def __init__(self):
    # self.search = OpenSearch(
    #   hosts = [{'host': host, 'port': 443}],
    #   http_auth = awsauth,
    #   use_ssl = True,
    #   verify_certs = True,
    #   connection_class = RequestsHttpConnection
    # )
    self.db = boto3.client('dynamodb', region_name=region, aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    self.unprocessed_cards = []

  def check_filename_in_search(self, filename, opt_prefix=None):
    index = f"{index_prefix}-*" if opt_prefix is None else opt_prefix

    response = self.search.search(index=index, body={
      "query": {
        "term": {
          "filename.keyword": filename
        }
      },
      "_source": False
    })

    return len(response['hits']['hits']) > 0
  
  def check_content_hash_in_search(self, content_hash):
    response = self.search.search(index="personal", body={
      "query": {
        "term": {
          "content_hash.keyword": content_hash
        }
      },
      "_source": False
    })

    return len(response['hits']['hits']) > 0
  
  def check_indexed(self, id):
    response = self.db.get_item(
      TableName=table_name,
      Key={
        'id': {
          'S': id
        }
      },
      ReturnConsumedCapacity='NONE',
      ProjectionExpression='id,download_url'
    )
    return 'Item' in response and 'download_url' in response['Item']

  def upload_cards(self, cards, force_upload=False, opt_prefix=None):
    card_objects = list(map(lambda card: card.get_index(), cards))

    if len(card_objects) == 0:
      return

    if card_objects[0].get("filename") is not None and not force_upload:
      filename = card_objects[0].get("filename")
      if self.check_filename_in_search(filename, opt_prefix=opt_prefix):
        print(f"{filename} already in search, skipping")
        return

    bulk_file = ""
    for card in card_objects:
      bulk_file += ('{ "index" : { "_index" : "') + \
        (opt_prefix if opt_prefix is not None else 
          (f"{index_prefix}-{card['division']}-{card['year']}" if card.get("division") is not None else index_prefix)) + \
        ('", "_type" : "_doc", "_id" : "' + str(card["id"]) + '" } }\n')

      bulk_file += json.dumps({i:card[i] for i in card if i != 'id'}) + '\n'

    if bulk_file is not None and len(bulk_file) > 0:
      self.search.bulk(body=bulk_file)
      print(f"Uploaded to OpenSearch: {card_objects[0].get('filename')}" if card_objects[0].get("filename") is not None else "Uploaded!")
  
  def upload_to_dynamo(self, cards):
    if len(cards) == 0:
      return
      
    self.unprocessed_cards.extend(list(map(lambda card: {"PutRequest": {"Item": card.get_dynamo()}}, cards)))
    to_process = self.unprocessed_cards[:25]
    self.unprocessed_cards = self.unprocessed_cards[25:]

    # de-duplicate to avoid BatchWriteItem errors
    acc = []
    for item in to_process:
      if not any(i['PutRequest']['Item']['id']['S'] == item['PutRequest']['Item']['id']['S'] for i in acc):
        acc.append(item)
    to_process = acc

    if len(to_process) > 0:
      response = self.db.batch_write_item(
        RequestItems={
          table_name: to_process
        }
      )

      unprocessed = response.get("UnprocessedItems", {}).get(table_name, [])
      self.unprocessed_cards.extend(unprocessed)
      print(f"Added to DynamoDB queue: {cards[0].additional_info.get('filename')}" if cards[0].additional_info.get('filename') is not None else "Uploaded!")

  # Clean up old cards from search and DynamoDB that are no longer in the Dropbox
  def remove_files(self, dropbox_files, account_id):
    # Get all the cards from search that don't have content hashes in the dropbox files
    response = self.search.search(index="personal", body={
      "query": {
        "bool": {
          "must_not": {
            "terms": {
              "content_hash.keyword": list(map(lambda file: file.get("content_hash", ""), dropbox_files))
            }
          },
          "must": {
            "match": {
              "team": account_id
            }
          }
        }
      },
      "_source": False,
      "size": 2000
    })

    num_cards = len(response['hits']['hits'])

    # Remove the cards from search
    bulk_file = ""
    for hit in response['hits']['hits']:
      bulk_file += ('{ "delete" : { "_index" : "personal", "_type" : "_doc", "_id" : "' + hit['_id'] + '" } }\n')

    if bulk_file is not None and len(bulk_file) > 0:
      self.search.bulk(body=bulk_file)
      print(f"Removed {num_cards} cards from OpenSearch")

    # Remove the cards from DynamoDB in batches of 25
    to_remove = list(map(lambda hit: {"DeleteRequest": {"Key": {"id": {"S": hit['_id']}}}}, response['hits']['hits']))
    while len(to_remove) > 0:
      batch = to_remove[:25]
      to_remove = to_remove[25:]

      db_response = self.db.batch_write_item(
        RequestItems={
          table_name: batch
        }
      )

      unprocessed = response.get("UnprocessedItems", {}).get(table_name, [])
      to_remove.extend(unprocessed)
    
    print(f"Removed {num_cards} cards from DynamoDB")
