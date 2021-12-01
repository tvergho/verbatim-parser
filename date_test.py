from dateutil import parser
import itertools
import re

def append_to_year_string(year):
  try:
    if int(year) <= 21:
      return "20" + str(year).zfill(2)
    else: return "19" + str(year)
  except:
    return year

def generate_date_from_cite(date_str, verbose=False):
  words = list(filter(lambda word : word.lower() != "and" and word.lower() != "or" and word.lower() != "of", map(lambda w : re.sub(r'[^a-zA-Z0-9/-]', '', w), date_str.split(" "))))
  words = list(itertools.takewhile(lambda word : word.lower() != "accessed", words))
  possibilities = []
  successes = []
  weights = []
  combos = [list(filter(lambda w : len(w) > 0, words[i:j])) for i, j in itertools.combinations(range(len(words)+1), 2) if j - i < 5]
  d_str = None

  for combo in combos:
    try:
      weight = 0
      
      for i in range(len(combo)):
        weight += len(combo[i])
        if (len(combo[i]) == 2 or len(combo[i]) == 1) and combo[i].isdecimal() and len(combo) == 1:
          weight -= len(combo[i])
          combo[i] = append_to_year_string(combo[i])
          if d_str is None:
            d_str = combo[i]
          weight += len(combo[i])

      if all(map(lambda c : (not c.isdecimal()) or len(c) < 4, combo)) and d_str is not None:
        combo.append(d_str)
        weight += len(d_str)

      possibilities.append(parser.parse(" ".join(combo)))

      successes.append(combo)
      weights.append(weight)
    except Exception as e:
      pass

  possibilities = [x for _, x in sorted(zip(weights, possibilities), reverse=True, key=lambda c : c[0])]
  possibilities = list(filter(lambda x : x.year > 1900, possibilities))

  date = possibilities[0]
  if verbose:
    print(successes)
    print(date.strftime("%m/%d/%Y"))

  return date