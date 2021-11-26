import requests
from bs4 import BeautifulSoup

wiki_url = "https://opencaselist.paperlessdebate.com/"

class Scraper:
  def __init__(self):
    main_page = requests.get(wiki_url)
    contents = main_page.content
    self.main_soup = BeautifulSoup(contents, "html.parser")
    self.school_links = list(filter(lambda el : el.name == "a", map(lambda el : el.contents[0], self.main_soup.find_all("span", class_="wikilink"))))
    self.school_links = list(map(lambda el : {"name": el.get_text(), "href": el.attrs["href"]}, self.school_links))

    print(self.school_links)

if __name__ == "__main__":
  scraper = Scraper()