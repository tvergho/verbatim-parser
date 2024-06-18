from dotenv import load_dotenv
import os
import boto3
import json
import pinecone
import cohere
import logging

load_dotenv()
pinecone.init(api_key=os.environ['PINECONE_KEY'], environment="us-west-2-aws")
index = pinecone.Index("logos-1718665984-index")
co = cohere.Client(os.environ['COHERE_KEY'])
logger = logging.getLogger('waitress')

namespace = "cards"
region = 'us-west-1'
table_name = 'logos-debate-pinecone'
gsi_name = 'team-content_hash-index'

class Search():
  def __init__(self):
    self.unprocessed_cards = []
    self.unprocessed_dynamo_cards = []
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

  # This is not working for some reason
  def check_content_hash_in_dynamo(self, account_id, content_hash):
    response = self.db.query(
      TableName=table_name,
      IndexName=gsi_name,
      KeyConditionExpression="team = :team and content_hash = :content_hash",
      ExpressionAttributeValues={
        ":team": {"S": account_id},
        ":content_hash": {"S": content_hash}
      },
      Select='COUNT'
    )
    item_count = response.get("Count", 0)
    return item_count > 0

  def upload_cards(self, cards, ns=None):
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
        namespace=namespace if ns is None else ns, 
        vectors=[(card['id'], embedding, card) for card, embedding in zip(to_upload, embeddings)]
      )

      print(f"Uploaded {len(to_upload)} cards to Pinecone")

  def upload_all_remaining_cards(self):
    print(f"{len(self.unprocessed_cards)} remaining")
    while len(self.unprocessed_cards) > 0:
      self.upload_cards([])
      print(f"{len(self.unprocessed_cards)} remaining")
    
    print(f"{len(self.unprocessed_dynamo_cards)} remaining")
    while len(self.unprocessed_dynamo_cards) > 0:
      self.upload_to_dynamo([])
      print(f"{len(self.unprocessed_dynamo_cards)} remaining")

  def upload_to_dynamo(self, cards):
    self.unprocessed_dynamo_cards.extend(list(map(lambda card: {"PutRequest": {"Item": card.get_dynamo()}}, cards)))
    to_process = self.unprocessed_dynamo_cards[:25]
    self.unprocessed_dynamo_cards = self.unprocessed_dynamo_cards[25:]

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
      self.unprocessed_dynamo_cards.extend(unprocessed)
      print(f"Uploaded {len(to_process)} cards to DynamoDB")
  
  def get_cards_by_team(self, team):
    cards = []

    # Initialize the pagination token
    last_evaluated_key = None

    while True:
        query_params = {
            'TableName': table_name,
            'IndexName': gsi_name,
            'KeyConditionExpression': 'team = :team',
            'ExpressionAttributeValues': {
                ':team': {'S': team}
            }
        }

        # If a pagination token is available, add it to the query parameters
        if last_evaluated_key:
            query_params['ExclusiveStartKey'] = last_evaluated_key

        # Perform the query
        response = self.db.query(**query_params)

        # Add the retrieved items to the cards list
        cards.extend(response['Items'])

        # Check if there's a LastEvaluatedKey to fetch the next set of items
        if 'LastEvaluatedKey' in response:
            last_evaluated_key = response['LastEvaluatedKey']
        else:
            break

    return cards

  # Clean up old cards from search and DynamoDB that are no longer in the Dropbox
  def remove_files(self, dropbox_files, account_id):
    # Get all the cards from search that correspond to the account
    cards = self.get_cards_by_team(account_id)

    # Get all the content_hash from dropbox_files
    dropbox_content_hashes = list(map(lambda file: file.get('content_hash', None), dropbox_files))
    dropbox_content_hashes = list(filter(lambda hash: hash is not None, dropbox_content_hashes))

    # Isolate cards that are not in dropbox_files
    cards = list(filter(lambda card: card['content_hash']['S'] not in dropbox_content_hashes, cards))
    
    # Get those IDs
    ids = list(map(lambda card: card['id']['S'], cards))

    # Remove those cards from DynamoDB in batches of 25
    to_remove = list(map(lambda hit: {"DeleteRequest": {"Key": {"id": {"S": hit}}}}, ids))

    num_cards = len(to_remove)
    logger.info(f"Removing {num_cards} cards")

    while len(to_remove) > 0:
      batch = to_remove[:25]
      to_remove = to_remove[25:]

      db_response = self.db.batch_write_item(
        RequestItems={
          table_name: batch
        }
      )

      unprocessed = db_response.get("UnprocessedItems", {}).get(table_name, [])
      to_remove.extend(unprocessed)
    
    logger.info(f"Removed {num_cards} cards from DynamoDB")

    # Remove from Pinecone
    if len(ids) > 0:
      index.delete(namespace=namespace, ids=ids)

    logger.info(f"Removed {num_cards} cards from Pinecone")
