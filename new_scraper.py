import requests
import time
import asyncio
import aiohttp
import traceback
import json
import sys
from os.path import exists
from os import listdir, makedirs
from local_parser import Parser
from search import Search
from urllib.parse import unquote
from itertools import takewhile
from multiprocessing import Pool

tmp_folder = "./tmp/"
download_doc = "download_urls.txt"

sys.setrecursionlimit(25000)
search = Search()

def parse_and_upload(folder, filename, additional_info):
  try:
    if search.check_filename_in_search(unquote(filename)):
      print(f"{filename} already in search, skipping")
      return

    parser = Parser(folder + filename, additional_info)
    cards = parser.parse()
    search.upload_cards(cards, True)
    search.upload_to_dynamo(cards)
    print(f"{filename} processed")
  except Exception as e:
    print(e)
    print(traceback.format_exc())

class Scraper:
  def __init__(self, division, year, folder, username=None, password=None):
    self.folder = folder + division + "/" + year + "/"

    try:
      makedirs(self.folder)
    except:
      pass

    self.division = division
    self.year = year
    self.api_prefix = division + year

    self.caselist_token = None
    self.cookies = {}

    if username and password:
      self.authenticate(username, password)

    self.schools = []
    self.load_schools()
    print("Schools loaded: " + str(len(self.schools)))

    self.download_urls = {}
    self.session = aiohttp.ClientSession(cookies=self.cookies)

  def load_schools(self):
    url = f"https://api.opencaselist.com/v1/caselists/{self.api_prefix}/schools"
    response = requests.get(url, cookies=self.cookies)
    self.schools = response.json()

  def authenticate(self, username, password):
    body = {
      "username": username,
      "password": password,
      "remember": True
    }

    login_request = requests.post(url="https://api.opencaselist.com/v1/login", json=body)
    login_json = login_request.json()
    token = login_json["token"]

    if not token:
      raise Exception("Login failed")

    self.caselist_token = token
    self.cookies = {
      "caselist_token": token
    }

  async def scrape(self):
    if len(self.schools) > 0:
      for school in self.schools:
        await self.scrape_school(school)

    f = open(self.folder + download_doc, "w")
    json.dump(self.download_urls, f)
    f.close()

  def load_download_urls(self):
    f = open(self.folder + download_doc, "r")
    self.download_urls = json.load(f)
    f.close()

  async def scrape_school(self, school):
    name = school["name"]
    school_page = await self.session.get(f"https://api.opencaselist.com/v1/caselists/{self.api_prefix}/schools/{name}/teams")
    teams = await school_page.json()

    await asyncio.gather(*[self.scrape_team(name, team["name"]) for team in teams])

    print("Scraped " + name)

  async def scrape_team(self, school, team):
    try:
      data = await self.session.get(f"https://api.opencaselist.com/v1/caselists/{self.api_prefix}/schools/{school}/teams/{team}/rounds?side=")
      rounds = await data.json()

      debate_rounds = []

      for debate_round in rounds:
        if not debate_round['opensource']:
          continue

        url = f"https://api.opencaselist.com/v1/download?path={debate_round['opensource']}"
        filename = debate_round['opensource'].split("/")[-1]
        debate_rounds.append([url, filename, school, team])
      
      await asyncio.gather(*[self.download_document(url, filename, school, team) for url, filename, school, team in debate_rounds])
    except Exception as e:
      await asyncio.sleep(10)
      print(e)
      return
  
  async def download_document(self, url, filename, school_name, team_name):
    self.download_urls[filename] = {
      "download_url": url,
      "school": school_name,
      "team": team_name
    }

    if exists(self.folder + filename):
      return

    doc = await self.session.get(url)

    with open(self.folder + filename, "wb") as f:
      while True:
        chunk = await doc.content.read(1024)
        if not chunk:
          break
        f.write(chunk)
    await asyncio.sleep(0.5)
  
  def upload_documents(self):
    return [self.upload_document(filename) for filename in listdir(self.folder) if filename.endswith(".docx")]
   
  def upload_document(self, filename):
    download_data = self.download_urls.get(filename)

    if not download_data:
      return

    school_name = download_data["school"]
    team_name = download_data["team"]
    download_url = download_data["download_url"]

    try:
      additional_info = {
        "filename": unquote(filename), 
        "division": self.division, 
        "year": self.year,
        "school": school_name,
        "team": team_name,
      }
      if download_url is not None:
        additional_info["download_url"] = download_url
      
      return [self.folder, filename, additional_info]
    except Exception as e:
      traceback.print_exc()

async def main():
  scraper = Scraper("ndtceda", "22", tmp_folder, username="votgfyustnmhfsksmw@tmmcv.net", password="votgfyustnmhfsksmw")
  await scraper.scrape()
  await scraper.session.close()

if __name__ == "__main__":
  loop = asyncio.get_event_loop()
  loop.run_until_complete(main())
