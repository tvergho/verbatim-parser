from multiprocessing import Pool
from new_scraper import download_doc, Scraper, parse_and_upload
from os.path import exists
from dotenv import load_dotenv
import asyncio
import sys
import os

load_dotenv()

tmp_folder = "./tmp/"

logins = {}

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

  # if exists(tmp_folder + division + "/" + year + "/" + download_doc):
  #   scraper.load_download_urls()
  # else:
  await scraper.scrape()

  # executables = scraper.upload_documents()
  # pool.starmap(parse_and_upload, executables)
  # pool.close()
  # pool.join()

  await scraper.close_all_sessions()

if __name__ == "__main__":
  load_logins()
  loop = asyncio.get_event_loop()
  loop.run_until_complete(process_downloaded_documents("ndtceda", "22"))
