from multiprocessing import Pool
from new_scraper import download_doc, Scraper
from os.path import exists
from dotenv import load_dotenv
import asyncio
import sys
import os
import argparse
from new_search import Search
from local_parser import Parser

load_dotenv()

tmp_folder = "./tmp/"
done_folder = "./done/"

logins = {}
search = Search()

def parse_and_upload(folder, filename, additional_info):
  try:
    print(folder + filename)
    parser = Parser(folder + filename, additional_info)
    cards = parser.parse()

    already_processed = []
    for card in cards:
      if search.check_card_in_search(card.object_id):
        already_processed.append(card.object_id)
    cards = list(filter(lambda card: card.object_id not in already_processed, cards))

    # Filter duplicates
    cards = list({card.object_id: card for card in cards}.values())

    search.upload_cards(cards)
    search.upload_to_dynamo(cards)
    print(f"{filename} processed")

    os.rename(folder + filename, done_folder + filename)
  except Exception as e:
    print(e)
    print(traceback.format_exc())

def keyboard_interrupt():
  search.upload_all_remaining_cards()

def load_logins():
  emails = {}
  passwords = {}
  for key, value in os.environ.items():
    if key.startswith("LOGIN_EMAIL_"):
      i = int(key.split("_")[2])
      emails[i] = value
    elif key.startswith("LOGIN_PASSWORD_"):
      i = int(key.split("_")[2])
      passwords[i] = value

  for i in emails.keys():
    logins[emails[i]] = passwords[i]
  
  print(f"Loaded {len(logins)} logins")

async def process_downloaded_documents(division, year):
  pool = Pool(processes=4)
  scraper = Scraper(division, year, tmp_folder, credentials=logins)

  if exists(tmp_folder + division + "/" + year + "/" + download_doc):
    scraper.load_download_urls()
  else:
    await scraper.scrape()
    await scraper.session.close()
    await scraper.close_all_sessions()

  executables = scraper.upload_documents()
  pool.starmap(parse_and_upload, executables)
  pool.close()
  pool.join()
    
  

if __name__ == "__main__":
  load_logins()
  loop = asyncio.get_event_loop()
  parser = argparse.ArgumentParser()
  parser.add_argument("--division", type=str, help="division to download (should correspond to opencaselist api)", default="ndtceda")
  parser.add_argument("--year", type=str, help="year to download", default="22")
  args = parser.parse_args()

  try:
    loop.run_until_complete(process_downloaded_documents("ndtceda", args.year))
  finally:
    keyboard_interrupt()
