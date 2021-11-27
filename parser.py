import sys
import os
from docx import Document
from card import TAG_NAME, Card
from search import Search

class Parser():
  def __init__(self, filename, additional_info={}):
    self.filename = filename
    self.document = Document(self.filename)
    self.cards = []
    self.additional_info = additional_info

  def parse(self):
    current_card = []

    for paragraph in self.document.paragraphs:
      if paragraph.style.name == TAG_NAME:
        try:
          self.cards.append(Card(current_card, self.additional_info))
        except:
          continue
        finally:
          current_card = [paragraph]
      else:
        current_card.append(paragraph)
    
    return self.cards

if __name__ == "__main__":
  if len(sys.argv) != 2:
    print("Usage: python3 parser.py <file.docx>")
    sys.exit(1)

  docx_name = sys.argv[1]
  if not os.path.isfile(docx_name):
    print("File not found")
    sys.exit(1)
  
  parser = Parser(docx_name, {"filename": docx_name})
  cards = parser.parse()

  search = Search()
  search.upload_cards(cards)
  search.upload_to_dynamo(cards)