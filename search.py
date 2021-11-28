from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3
import json

host = 'search-logos-test-liwqw3bnlb3qyn3kvfhxyqkldi.us-west-1.es.amazonaws.com' # For example, my-test-domain.us-east-1.es.amazonaws.com
region = 'us-west-1'
table_name = 'logos-debate'
bucket_name = "logos-debate"
index_prefix = "cards"

service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service)
class Search():
  def __init__(self):
    self.search = OpenSearch(
      hosts = [{'host': host, 'port': 443}],
      http_auth = awsauth,
      use_ssl = True,
      verify_certs = True,
      connection_class = RequestsHttpConnection
    )
    self.db = boto3.client('dynamodb', region_name=region)
    self.unprocessed_cards = []

  def check_filename_in_search(self, filename):
    response = self.search.search(index=f"{index_prefix}-*", body={
      "query": {
        "match": {
          "filename": filename
        }
      },
      "_source": False
    })
    return response["hits"]["total"]["value"] > 0

  def upload_cards(self, cards, force_upload=False):
    card_objects = list(map(lambda card: card.get_index(), cards))

    if len(card_objects) == 0:
      return

    if card_objects[0].get("filename") is not None and not force_upload:
      filename = card_objects[0].get("filename")
      if self.check_filename_in_search(filename):
        print(f"{filename} already in search, skipping")
        return

    bulk_file = ""
    for card in card_objects:
      bulk_file += ('{ "index" : { "_index" : "') + \
        (f"{index_prefix}-{card['division']}-{card['year']}" if card.get("division") is not None else index_prefix) + \
        ('", "_type" : "_doc", "_id" : "' + str(card["id"]) + '" } }\n')

      bulk_file += json.dumps({i:card[i] for i in card if i != 'id'}) + '\n'

    self.search.bulk(body=bulk_file)
    print(f"Uploaded to OpenSearch: {card_objects[0].get('filename')}" if card_objects[0].get("filename") is not None else "Uploaded!")
  
  def upload_to_dynamo(self, cards):
    if len(cards) == 0:
      return
      
    self.unprocessed_cards.extend(list(map(lambda card: {"PutRequest": {"Item": card.get_dynamo()}}, cards)))
    to_process = self.unprocessed_cards[:25]
    self.unprocessed_cards = self.unprocessed_cards[25:]

    response = self.db.batch_write_item(
      RequestItems={
        table_name: to_process
      }
    )

    unprocessed = response.get("UnprocessedItems", {}).get(table_name, [])
    self.unprocessed_cards.extend(unprocessed)

    print(f"Added to DynamoDB queue: {cards[0].additional_info.get('filename')}" if cards[0].additional_info.get('filename') is not None else "Uploaded!")