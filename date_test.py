import datetime
import re


MIN_EVIDENCE_YEAR = 1900
ACCESS_MARKER = re.compile(r"\b(?:accessed|retrieved|date\s+accessed|doa)\b", re.IGNORECASE)


def normalize_short_year(value, current_year):
  """Map a one/two-digit cite year into the nearest non-future century."""
  short = int(value)
  candidate = (current_year // 100) * 100 + short
  if candidate > current_year + 1:
    candidate -= 100
  return candidate


def extract_evidence_year(text, current_year):
  for match in re.finditer(r"(?<!\d)(1\d{3}|20\d{2})(?!\d)", text):
    year = int(match.group(1))
    if MIN_EVIDENCE_YEAR <= year <= current_year + 1:
      return year

  apostrophe = re.search(r"[\u2018\u2019'](\d{1,2})(?!\d)", text)
  if apostrophe:
    year = normalize_short_year(apostrophe.group(1), current_year)
    if MIN_EVIDENCE_YEAR <= year <= current_year + 1:
      return year

  # Bare two-digit years are common immediately after an author name. Limit
  # this fallback to the beginning of the cite so page and volume numbers do
  # not masquerade as dates.
  bare = re.search(r"(?<!\d)(\d{2})(?!\d)", text[:100])
  if bare:
    year = normalize_short_year(bare.group(1), current_year)
    if MIN_EVIDENCE_YEAR <= year <= current_year + 1:
      return year
  return None


def generate_date_from_cite(
  date_str,
  emphasized_ranges=None,
  current_year=None,
  verbose=False,
):
  """Return a conservative evidence date, never an access or caselist date.

  Verbatim's F8/13pt-bold cite spans are authoritative when present. The
  fallback searches only the pre-access citation text and intentionally
  returns January 1 when only a year can be proven.
  """
  if not isinstance(date_str, str) or not date_str.strip():
    return None
  current_year = current_year or datetime.date.today().year

  emphasized_text = " ".join(
    date_str[start:end]
    for start, end in emphasized_ranges or []
    if isinstance(start, int) and isinstance(end, int) and 0 <= start < end <= len(date_str)
  )
  year = extract_evidence_year(emphasized_text, current_year) if emphasized_text else None
  if year is None:
    pre_access = ACCESS_MARKER.split(date_str, maxsplit=1)[0]
    year = extract_evidence_year(pre_access, current_year)
  if year is None:
    return None

  result = datetime.date(year, 1, 1)
  if verbose:
    print(result.strftime("%m/%d/%Y"))
  return result
