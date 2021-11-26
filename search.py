from algoliasearch.search_client import SearchClient

app_id = "BWL4RCMC8P"
api_key = "43bbdea4956278acb847051efff6345c"
index_name = "cards_test"

class Search():
  def __init__(self):
    self.client = SearchClient.create(app_id, api_key)
    self.index = self.client.init_index(index_name)

  def upload_cards(self, cards):
    card_objects = map(lambda card: card.get_index(), cards)
    res = self.index.save_objects(card_objects)
    print("Uploaded!")