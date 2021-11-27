import hashlib

TAG_NAME = "Heading 4"
NORMAL_NAME = "Normal"
EMPHASIS_NAME = "Emphasis"
UNDERLINE_NAME = "Underline"

class Card():
  def __init__(self, paragraphs, additional_info):
    if paragraphs[0].style.name != TAG_NAME or len(paragraphs) < 2:
      raise Exception("Invalid paragraph structure")

    self.paragraphs = paragraphs
    self.tag = paragraphs[0].text.strip(", ")
    self.cite = paragraphs[1].text
    self.body = [p.text for p in paragraphs[2:] if p.style.name == NORMAL_NAME]

    if len(self.body) == 0:
      raise Exception("Card is too short")

    self.highlights = []
    self.emphasis = []
    self.underlines = []
    self.parse_paragraphs()

    self.additional_info = additional_info
    self.object_id = hashlib.sha256(str(self).encode()).hexdigest()

  def parse_paragraphs(self):
    for i in range(2, len(self.paragraphs)):
      p = self.paragraphs[i]
      runs = p.runs
      j = 0

      for r in runs:
        run_text = r.text.strip()
        run_index = p.text.find(run_text, j)

        if run_index == -1:
          continue
        if r.font.highlight_color is not None:
          self.highlights.append((i, run_index, run_index + len(run_text)))
        if UNDERLINE_NAME in r.style.name:
          self.underlines.append((i, run_index, run_index + len(run_text)))
        if EMPHASIS_NAME in r.style.name:
          self.emphasis.append((i, run_index, run_index + len(run_text)))
        
        j = run_index + len(run_text)
  
  def get_index(self):
    return {
      "tag": self.tag,
      "cite": self.cite,
      "body": self.body,
      "id": self.object_id,
      **self.additional_info
    }
  
  def get_dynamo(self):
    db_representation = {
      "tag": {"S": self.tag},
      "cite": {"S": self.cite},
      "body": {"L": [{"S":p} for p in self.body]},
      "highlights": { "L": [ { "L": [ {"N":str(i)} for i in v ] } for v in self.highlights ] },
      "emphasis": {"L": [{"L": [{"N":str(i)} for i in v]} for v in self.emphasis]},
      "underlines": {"L": [{"L": [{"N":str(i)} for i in v]} for v in self.underlines]},
      "id": {"S": self.object_id}
    }

    if self.additional_info.get("division") is not None and self.additional_info.get("year") is not None:
      db_representation["division"] = {"S": self.additional_info["division"]}
      db_representation["year"] = {"S": self.additional_info["year"]}
      db_representation["s3_url"] = {"S": f"https://logos-debate.s3.amazonaws.com/{self.additional_info['division']}/{self.additional_info['year']}/{self.additional_info['filename']}"}
        
    return db_representation

  def __str__(self):
    return f"{self.tag}\n{self.cite}\n{self.body}\n"

  def __repr__(self):
    return f"\n{self.tag}\n{self.cite}\n{self.body}\n"