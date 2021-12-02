from dateutil import parser
import itertools
import re
import datetime

# Cliff Mass 19. American professor of Atmospheric Sciences at the University of Washington. His research focuses on numerical weather modeling and prediction, the role of topography in the evolution of weather systems, regional climate modeling, and the weather of the Pacific Northwest. 8-12-19. “Is Global Warming an Existential Threat? Probably Not, But Still a Serious Issue.” https://cliffmass.blogspot.com/2019/08/is-global-warming-existential-threat.html. DOA: 3-25-2020. kyujin. Edited for gendered language [denoted with brackets]

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

      possibilities.append(parser.parse(" ".join(combo), default=datetime.datetime(2000, 1, 1)))

      successes.append(combo)
      weights.append(weight)
    except Exception as e:
      pass

  possibilities = [x for _, x in sorted(zip(weights, possibilities), reverse=True, key=lambda c : c[0])]
  possibilities = list(filter(lambda x : x.year > 1900, possibilities))

  if len(possibilities) == 0:
    return None

  date = possibilities[0]
  if verbose:
    print(successes)
    print(date.strftime("%m/%d/%Y"))

  return date.date() if date is not None else None
