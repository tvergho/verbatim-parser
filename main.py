from multiprocessing import Pool
from scraper import download_doc, Scraper, parse_and_upload
from os.path import exists
import asyncio
import sys

tmp_folder = "./tmp/"
pool = Pool(processes=4)

async def process_downloaded_documents(division, year):
  scraper = Scraper(division, year, tmp_folder, username="votgfyustnmhfsksmw@tmmcv.net", password="votgfyustnmhfsksmw")

  if exists(tmp_folder + division + "/" + year + "/" + download_doc):
    scraper.load_download_urls()
  else:
    await scraper.scrape()

  executables = scraper.upload_documents()
  pool.starmap(parse_and_upload, executables)
  pool.close()
  pool.join()

  await scraper.session.close()

loop = asyncio.get_event_loop()
loop.run_until_complete(process_downloaded_documents("ndtceda", "22"))
