from dotenv import load_dotenv
import os
import boto3
import json
import pinecone
import cohere

load_dotenv()
pinecone.init(api_key=os.environ['PINECONE_KEY'], environment="us-east-1-aws")
index = pinecone.Index("logos")
co = cohere.Client(os.environ['COHERE_KEY'])

namespace = "cards"
region = 'us-west-1'
table_name = 'logos-debate'

class Search():
  def __init__(self):
    self.unprocessed_cards = []
    self.db = boto3.client('dynamodb', region_name=region, aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])

  def check_card_in_search(self, id):
    response = index.query(
        namespace=namespace,
        top_k=10,
        include_values=False,
        include_metadata=True,
        id=id
    )
    return 'matches' in response and len(response.matches) > 0

  def upload_cards(self, cards):
    card_objects = list(map(lambda card: card.get_index(), cards))
    self.unprocessed_cards.extend(card_objects)
    to_upload = self.unprocessed_cards[:96]
    self.unprocessed_cards = self.unprocessed_cards[96:]

    if len(to_upload) > 0:
      cohere_response = co.embed(
        texts=list(map(lambda card: f"{card['tag']} {card['cite']} {card['highlighted_text']}", to_upload)), 
        truncate="END"
      )
      embeddings = cohere_response.embeddings
      index.upsert(
        namespace=namespace, 
        vectors=[(card['id'], embedding, card) for card, embedding in zip(to_upload, embeddings)]
      )

      print(f"Uploaded {len(to_upload)} cards to Pinecone")

  def upload_all_remaining_cards(self):
    print(f"{len(self.unprocessed_cards)} remaining")
    while len(self.unprocessed_cards) > 0:
      self.upload_cards([])
      print(f"{len(self.unprocessed_cards)} remaining")
  
  
  # Clean up old cards from search and DynamoDB that are no longer in the Dropbox
  # def remove_files(self, dropbox_files, account_id):
  #   # Get all the cards from search that don't have content hashes in the dropbox files
  #   response = self.search.search(index="personal", body={
  #     "query": {
  #       "bool": {
  #         "must_not": {
  #           "terms": {
  #             "content_hash.keyword": list(map(lambda file: file.get("content_hash", ""), dropbox_files))
  #           }
  #         },
  #         "must": {
  #           "match": {
  #             "team": account_id
  #           }
  #         }
  #       }
  #     },
  #     "_source": False,
  #     "size": 2000
  #   })

  #   num_cards = len(response['hits']['hits'])

  #   # Remove the cards from search
  #   bulk_file = ""
  #   for hit in response['hits']['hits']:
  #     bulk_file += ('{ "delete" : { "_index" : "personal", "_type" : "_doc", "_id" : "' + hit['_id'] + '" } }\n')

  #   if bulk_file is not None and len(bulk_file) > 0:
  #     self.search.bulk(body=bulk_file)
  #     print(f"Removed {num_cards} cards from OpenSearch")

  #   # Remove the cards from DynamoDB in batches of 25
  #   to_remove = list(map(lambda hit: {"DeleteRequest": {"Key": {"id": {"S": hit['_id']}}}}, response['hits']['hits']))
  #   while len(to_remove) > 0:
  #     batch = to_remove[:25]
  #     to_remove = to_remove[25:]

  #     db_response = self.db.batch_write_item(
  #       RequestItems={
  #         table_name: batch
  #       }
  #     )

  #     unprocessed = response.get("UnprocessedItems", {}).get(table_name, [])
  #     to_remove.extend(unprocessed)
    
  #   print(f"Removed {num_cards} cards from DynamoDB")
