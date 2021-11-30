from dateutil import parser
import itertools
import re

# "Kerry Lynn Macintosh 97, Associate Professor of Law, Santa Clara University School of Law. B.A. 1978, Pomona College; J.D. 1982, Stanford University, “Liberty, Trade, and the Uniform Commercial Code: When Should Default Rules Be Based On Business Practices?,” 38 Wm. & Mary L. Rev. 1465, Lexis"
# Philip C. Kissam 83, Professor of Law, University of Kansas. B.A. 1963, Amherst College; LL.B. 1968, Yale University. "Antitrust Law and Professional Behavior", 62 Tex. L. Rev. 1

date_str = "Philip C. Kissam 83, Professor of Law, University of Kansas. B.A. 1963, Amherst College; LL.B. 1968, Yale University. Antitrust Law and Professional Behavior, 62 Tex. L. Rev. 1"
words = list(filter(lambda word : word.lower() != "and" and word.lower() != "or" and word.lower() != "of", map(lambda w : re.sub(r'[^a-zA-Z0-9/-]', '', w), date_str.split(" "))))
words = list(itertools.takewhile(lambda word : word.lower() != "accessed", words))
possibilities = []
successes = []
weights = []
combos = [list(filter(lambda w : len(w) > 0, words[i:j])) for i, j in itertools.combinations(range(len(words)+1), 2) if j - i < 5]

def append_to_year_string(year):
  try:
    if int(year) <= 21:
      return "20" + str(year).zfill(2)
    else: return "19" + str(year)
  except:
    return year

for combo in combos:
  try:
    weight = 0
    for i in range(len(combo)):
      weight += len(combo[i])
      if (len(combo[i]) == 2 or len(combo[i]) == 1) and combo[i].isdecimal() and len(combo) == 1:
        weight -= len(combo[i])
        combo[i] = append_to_year_string(combo[i])
        weight += len(combo[i])

    possibilities.append(parser.parse(" ".join(combo)))

    successes.append(combo)
    weights.append(weight)
  except Exception as e:
    pass

possibilities = [x for _, x in sorted(zip(weights, possibilities), reverse=True)]
possibilities = list(filter(lambda x : x.year > 1900, possibilities))

date = possibilities[0]
print(possibilities)
print(successes)
print(date.strftime("%m/%d/%Y"))