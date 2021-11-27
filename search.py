from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3
import json

host = 'search-logos-test-liwqw3bnlb3qyn3kvfhxyqkldi.us-west-1.es.amazonaws.com' # For example, my-test-domain.us-east-1.es.amazonaws.com
region = 'us-west-1'
table_name = 'logos-debate'
bucket_name = "logos-debate"

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

  def upload_cards(self, cards):
    card_objects = list(map(lambda card: card.get_index(), cards))
    bulk_file = ""
    for card in card_objects:
      bulk_file += ('{ "index" : { "_index" : "cards') + \
        (f"-{card['division']}-{card['year']}" if card.get("division") is not None else "") + \
        ('", "_type" : "_doc", "_id" : "' + str(card["id"]) + '" } }\n')

      bulk_file += json.dumps({i:card[i] for i in card if i != 'id'}) + '\n'

    self.search.bulk(body=bulk_file)
    print(f"Uploaded to OpenSearch: {card_objects[0].get('filename')}" if card_objects[0].get("filename") is not None else "Uploaded!")
  
  def upload_to_dynamo(self, cards):
    self.db.batch_write_item(
      RequestItems={
        table_name: list(map(lambda card: {"PutRequest": {"Item": card.get_dynamo()}}, cards))
      }
    )
    print(f"Uploaded to DynamoDB: {cards[0].additional_info.get('filename')}" if cards[0].additional_info.get('filename') is not None else "Uploaded!")