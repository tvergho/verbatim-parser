import requests
import time
import asyncio
import aiohttp
import traceback
import json
import sys
from bs4 import BeautifulSoup
from os.path import exists
from os import listdir
from parser import Parser
from search import Search, bucket_name
from urllib.parse import unquote
from itertools import takewhile
from multiprocessing import Pool

wiki_url = "https://opencaselist.paperlessdebate.com"
tmp_folder = "./tmp/"

sys.setrecursionlimit(25000)
search = Search()

def parse_and_upload(folder, filename, additional_info):
  try:
    parser = Parser(folder + filename, additional_info)
    print("Parsing " + filename)
    cards = parser.parse()
    search.upload_cards(cards, True)
    print("Indexed " + filename)
    search.upload_to_dynamo(cards)
    print("Uploaded to DynamoDB: " + filename)
  except Exception as e:
    print(e)
    print(traceback.format_exc())
class Scraper:
  def __init__(self, url, division, year, folder):
    self.url = url
    self.folder = folder
    self.dir_name = division + "/" + year + "/"
    self.division = division
    self.year = year

    main_page = requests.get(url)
    contents = main_page.content
    self.main_soup = BeautifulSoup(contents, "html.parser")
    schools_pane = self.main_soup.find("div", class_="PanelsSchools")

    self.schools = list(filter(lambda el : el.name == "a", map(lambda el : el.contents[0], schools_pane.find_all("span", class_="wikilink"))))
    self.schools = list(map(lambda el : {"name": el.get_text(), "href": el.attrs["href"]}, self.schools))

    self.download_urls = {}
    self.session = aiohttp.ClientSession()

  async def scrape(self):
    await asyncio.gather(*[self.scrape_school(school) for school in self.schools])
    f = open(self.folder + "download_urls.txt", "w")
    json.dump(self.download_urls, f)
    f.close()

  def load_download_urls(self):
    f = open(self.folder + "download_urls.txt", "r")
    self.download_urls = json.load(f)
    f.close()

  async def scrape_school(self, school):
    href = school["href"]
    name = school["name"]
    school_page = await self.session.get(wiki_url + href)
    contents = await school_page.read()
    soup = BeautifulSoup(contents.decode('utf-8'), "html.parser")

    teams_table = soup.find("table", id="tblTeams").contents
    teams = teams_table[1:]
    teams = list(map(lambda el : [el.contents[1].find("a").attrs["href"], el.contents[2].find("a").attrs["href"]], teams))
    
    aff_urls = []
    neg_urls = []
    for team in teams:
      aff_url = wiki_url + team[0]
      neg_url = wiki_url + team[1]
      aff_urls.append(aff_url)
      neg_urls.append(neg_url)

    await asyncio.gather(*[self.scrape_wiki_page(aff_url) for aff_url in aff_urls])
    await asyncio.gather(*[self.scrape_wiki_page(neg_url) for neg_url in neg_urls])

    print("Scraped " + name)

  async def scrape_wiki_page(self, url):
    page = await self.session.get(url)
    contents = await page.read()
    soup = BeautifulSoup(contents.decode('utf-8'), "html.parser")
    links = set(filter(lambda href : "docx" in href, map(lambda el : el.contents[0].attrs["href"], soup.find_all("span", class_="wikiexternallink"))))

    try:
      await asyncio.gather(*[self.download_document(url) for url in links])
      await asyncio.sleep(0.5)
    except Exception as e:
      print(e)

    print("Scraped " + url)
  
  async def download_document(self, url):
    filename = url.split("/")[-1].split("?")[0]

    self.download_urls[filename] = url

    if exists(self.folder + filename):
      return

    doc = requests.get(url, stream=True)
    with open(self.folder + filename, "wb") as f:
      for chunk in doc.iter_content(chunk_size=1024):
        if chunk:
          f.write(chunk)
    return filename
  
  def upload_documents(self):
    return [self.upload_document(filename) for filename in listdir(self.folder) if filename.endswith(".docx")]
  
  def get_school_name(self, filename):
    school = ""
    for part in unquote(filename).split("-"):
      if part.lower() in ["aff", "neg"]: return ""
      school += part
      if True in [s["name"] == school for s in self.schools]: return school
      school += " "
    return ""
  
  def get_team_name(self, filename, school_name):
    team_name = ""

    file_parts = unquote(filename).replace(" ", "-").split("-")
    file_parts = list(filter(lambda part : part not in school_name, file_parts))

    for (i,part) in enumerate(takewhile(lambda part : part.lower() != "aff" and part.lower() != "neg", file_parts)):
      if i > 0: team_name += "-"
      team_name += part
    
    return team_name
   
  def upload_document(self, filename):
    school_name = self.get_school_name(filename)
    team_name = self.get_team_name(filename, school_name)
    download_url = self.download_urls.get(filename)

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

if __name__ == "__main__":
  pool = Pool(processes=8)

  scraper = Scraper(wiki_url, "college", "21-22", tmp_folder)
  scraper.load_download_urls()
  executables = scraper.upload_documents()
  pool.starmap(parse_and_upload, executables)
  pool.close()
  pool.join()
