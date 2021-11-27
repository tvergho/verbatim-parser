import requests
import time
import asyncio
import boto3
from bs4 import BeautifulSoup
from os.path import exists
from os import listdir
from parser import Parser
from search import Search

wiki_url = "https://opencaselist.paperlessdebate.com"
tmp_folder = "./tmp/"
bucket_name = "logos-debate"

s3Client = boto3.client('s3')
s3 = boto3.resource('s3')
class Scraper:
  def __init__(self, url, division, year):
    self.url = url
    self.dir_name = division + "/" + year + "/"
    self.division = division
    self.year = year

    main_page = requests.get(url)
    contents = main_page.content
    self.main_soup = BeautifulSoup(contents, "html.parser")
    schools_pane = self.main_soup.find("div", class_="PanelsSchools")

    self.schools = list(filter(lambda el : el.name == "a", map(lambda el : el.contents[0], schools_pane.find_all("span", class_="wikilink"))))
    self.schools = list(map(lambda el : {"name": el.get_text(), "href": el.attrs["href"]}, self.schools))

  async def scrape(self):
    for school in self.schools:
      await self.scrape_school(school)

  async def scrape_school(self, school):
    href = school["href"]
    name = school["name"]
    school_page = requests.get(wiki_url + href)
    contents = school_page.content
    soup = BeautifulSoup(contents, "html.parser")

    teams_table = soup.find("table", id="tblTeams").contents
    teams = teams_table[1:]
    teams = list(map(lambda el : [el.contents[1].find("a").attrs["href"], el.contents[2].find("a").attrs["href"]], teams))
    
    for team in teams:
      aff_url = wiki_url + team[0]
      neg_url = wiki_url + team[1]

      await asyncio.gather(self.scrape_wiki_page(aff_url), self.scrape_wiki_page(neg_url))
      await asyncio.sleep(0.5)

  async def scrape_wiki_page(self, url):
    page = requests.get(url)
    contents = page.content
    soup = BeautifulSoup(contents, "html.parser")
    links = set(filter(lambda href : "docx" in href, map(lambda el : el.contents[0].attrs["href"], soup.find_all("span", class_="wikiexternallink"))))

    try:
      await asyncio.gather(*[self.download_document(url) for url in links])
    except Exception as e:
      print(e)

    print("Scraped " + url)
  
  async def download_document(self, url):
    doc = requests.get(url, stream=True)
    filename = url.split("/")[-1].split("?")[0]
    if exists(tmp_folder + filename):
      return

    with open(tmp_folder + filename, "wb") as f:
      for chunk in doc.iter_content(chunk_size=1024):
        if chunk:
          f.write(chunk)
    return filename
  
  async def upload_documents(self, folder):
    search = Search()
    await asyncio.gather(*[self.upload_document(filename, search) for filename in listdir(folder)])
   
  async def upload_document(self, filename, search):
    key = self.dir_name + filename
    loop = asyncio.get_event_loop()

    try: 
      s3.Object(bucket_name, key).load()
    except Exception as e:
      await loop.run_in_executor(None, s3Client.upload_file, folder + filename, bucket_name, key, ExtraArgs={"ACL": "public-read"})
      print("Uploaded " + filename)
    finally:
      def parse_and_index(): 
        parser = Parser(tmp_folder + filename, {"filename": filename, "division": self.division, "year": self.year})
        print("Parsing " + filename)
        cards = parser.parse()
        search.upload_cards(cards)

      await loop.run_in_executor(None, parse_and_index)

if __name__ == "__main__":
  scraper = Scraper(wiki_url, "college", "21-22")
  # asyncio.run(scraper.scrape())
  asyncio.run(scraper.upload_documents(folder=tmp_folder))