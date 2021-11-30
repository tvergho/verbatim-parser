from multiprocessing import Pool
from scraper import download_doc, Scraper, parse_and_upload
from os.path import exists
import asyncio
import sys

wiki_url = "https://opencaselist20.paperlessdebate.com"
tmp_folder = "./tmp/"

pool = Pool(processes=8)

async def process_downloaded_documents(url, division, year):
  scraper = Scraper(url, division, year, tmp_folder)

  if exists(tmp_folder + division + "/" + year + "/" + download_doc):
    scraper.load_download_urls()
  else:
    await scraper.scrape()

  executables = scraper.upload_documents()
  pool.starmap(parse_and_upload, executables)
  pool.close()
  pool.join()

  await scraper.session.close()

asyncio.run(process_downloaded_documents(wiki_url, "college", "20-21"))
