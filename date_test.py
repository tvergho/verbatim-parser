from dateutil import parser
import itertools
import re

date_str = "Kerry Lynn Macintosh 97, Associate Professor of Law, Santa Clara University School of Law. B.A. 1978, Pomona College; J.D. 1982, Stanford University, “Liberty, Trade, and the Uniform Commercial Code: When Should Default Rules Be Based On Business Practices?,” 38 Wm. & Mary L. Rev. 1465, Lexis"
words = list(filter(lambda word : word.lower() != "and" and word.lower() != "or" and word.lower() != "of", map(lambda w : re.sub(r'[^a-zA-Z0-9/]', '', w.replace('’', '20')), date_str.split(" "))))
possibilities = []
successes = []
combos = words + list(itertools.combinations(words, 2)) + list(itertools.combinations(words, 3)) + list(itertools.combinations(words, 4))

for combo in combos:
  try:
    if len(combo[0]) > 1 or len(combo) > 1:
      possibilities.append(parser.parse(" ".join(combo)))
      successes.append(combo)
  except:
    pass

date = possibilities[0]
# print(successes)
print(date.strftime("%m/%d/%Y"))