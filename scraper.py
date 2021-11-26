import requests
from bs4 import BeautifulSoup

wiki_url = "https://opencaselist.paperlessdebate.com"

class Scraper:
  def __init__(self, url):
    self.url = url
    main_page = requests.get(url)
    contents = main_page.content
    self.main_soup = BeautifulSoup(contents, "html.parser")
    schools_pane = self.main_soup.find("div", class_="PanelsSchools")

    self.schools = list(filter(lambda el : el.name == "a", map(lambda el : el.contents[0], schools_pane.find_all("span", class_="wikilink"))))
    self.schools = list(map(lambda el : {"name": el.get_text(), "href": el.attrs["href"]}, self.schools))

  def scrape_school(self, school):
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

      self.scrape_wiki_page(aff_url)
      self.scrape_wiki_page(neg_url)

  def scrape_wiki_page(self, url):
    page = requests.get(url)
    contents = page.content
    soup = BeautifulSoup(contents, "html.parser")
    links = set(filter(lambda href : "docx" in href, map(lambda el : el.contents[0].attrs["href"], soup.find_all("span", class_="wikiexternallink"))))

    for doc_url in links:
      try:
        self.download_document(doc_url)
      except Exception as e:
        print(e)
        pass
  
  def download_document(self, url):
    doc = requests.get(url, stream=True)
    filename = url.split("/")[-1].split("?")[0]
    with open("./tmp/" + filename, "wb") as f:
      for chunk in doc.iter_content(chunk_size=1024):
        if chunk:
          f.write(chunk)


if __name__ == "__main__":
  scraper = Scraper(wiki_url)
  scraper.scrape_school(scraper.schools[0])