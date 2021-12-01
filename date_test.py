from dateutil import parser
import itertools
import re

# Grabow '19 - Policy analyst at the Cato Institute’s Herbert A. Stiefel Center for Trade Policy Studies [Colin, Nov 12, Rust Buckets: How the Jones Act Undermines U.S. Shipbuilding and National Security, https://www.cato.org/policy-analysis/rust-buckets-how-jones-act-undermines-us-shipbuilding-national-security]

date_str = "Robert Grosse et. al, 3-3-2021, Affiliations Thunderbird School of Global Management, Arizona State University, Phoenix, AZ, USA Robert Grosse, Jonas Gamso & Roy C. Nelson, China’s Rise, World Order, and the Implications for International Business, Management International Review, https://link.springer.com/article/10.1007/s11575-020-00433-8; accessed 8-24-2021"
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

possibilities = [x for _, x in sorted(zip(weights, possibilities), reverse=True, key=lambda c : c[0])]
possibilities = list(filter(lambda x : x.year > 1900, possibilities))

date = possibilities[0]
print(possibilities)
print(successes)
print(date.strftime("%m/%d/%Y"))