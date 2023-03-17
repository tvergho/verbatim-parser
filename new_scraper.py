import requests
import time
import asyncio
import aiohttp
import traceback
import json
import sys
from os.path import exists, getsize
from os import listdir, makedirs
from local_parser import Parser
from new_search import Search
from urllib.parse import unquote, quote
from itertools import takewhile
from multiprocessing import Pool

tmp_folder = "./tmp/"
download_doc = "download_urls.txt"

sys.setrecursionlimit(25000)

class Scraper:
  def __init__(self, division, year, folder, username=None, password=None, credentials=None):
    self.folder = folder + division + "/" + year + "/"

    try:
      makedirs(self.folder)
    except:
      pass

    self.division = division
    self.year = year
    self.api_prefix = division + year

    self.tokens = {}
    self.cookies = {}

    if username and password:
      self.authenticate(username, password)
    elif credentials:
      for username, password in credentials.items():
        self.authenticate(username, password)

    self.schools = []
    self.load_schools()
    print("Schools loaded: " + str(len(self.schools)))

    self.download_urls = {}

    self.sessions = []
    for token in self.tokens.values():
      self.sessions.append(aiohttp.ClientSession(cookies={"caselist_token": token}))

    self.session = self.sessions[0]

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
    print(login_json)
    token = login_json["token"]

    if not token:
      raise Exception("Login failed")

    self.tokens[username] = token
    self.cookies["caselist_token"] = token

  async def close_all_sessions(self):
    for session in self.sessions:
      await session.close()

  async def scrape(self):
    if len(self.schools) > 0:
      for school in self.schools:
        try:
          await self.scrape_school(school)
        except:
          print("Rate limit reached, waiting 60 seconds")
          await asyncio.sleep(60)
          await self.scrape()

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

        url = f"https://api.opencaselist.com/v1/download?path={quote(debate_round['opensource'])}"
        filename = debate_round['opensource'].split("/")[-1]
        debate_rounds.append([url, filename, school, team])
      
      # Evenly assign session indexes

      await asyncio.gather(*[self.download_document(url, filename, school, team, session_index=(i % len(self.sessions))) for i, (url, filename, school, team) in enumerate(debate_rounds)])
    except Exception as e:
      await asyncio.sleep(10)
      print(e)
      return
  
  async def download_document(self, url, filename, school_name, team_name, force_download=False, session_index=0):
    self.download_urls[filename] = {
      "download_url": url,
      "school": school_name,
      "team": team_name
    }

    if exists(self.folder + filename) and not force_download and getsize(self.folder + filename) > 1000:
      return

    session = self.sessions[session_index]
    doc = await session.get(url)

    with open(self.folder + filename, "wb") as f:
      while True:
        chunk = await doc.content.read(1024)
        if not chunk:
          break

        try:
          data = json.loads(chunk)
          if "You can only download 10 files per minute." in data["message"]:
            if session_index == len(self.sessions) - 1:
              print("Rate limit reached, waiting 60 seconds")
              await asyncio.sleep(60)
              await self.download_document(url, filename, school_name, team_name, force_download=True, session_index=0)
            else:
              print(f"Rate limit reached, retrying with session {session_index + 1}")
              await self.download_document(url, filename, school_name, team_name, force_download=True, session_index=session_index + 1)
            return
          elif "File not found" in data["message"]:
            print(f"{filename} not found for url {url}")
            return
        except:
          pass

        f.write(chunk)
  
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
