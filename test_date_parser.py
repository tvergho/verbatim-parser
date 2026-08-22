import datetime
import unittest

from date_test import generate_date_from_cite


class CitationDateTests(unittest.TestCase):
    def test_modern_two_digit_year_is_2022(self):
        self.assertEqual(
            generate_date_from_cite("Salem 22, Gramsci in the Postcolony"),
            datetime.date(2022, 1, 1),
        )

    def test_apostrophe_year_is_supported(self):
        self.assertEqual(
            generate_date_from_cite("Doran '09, Fooling Oneself"),
            datetime.date(2009, 1, 1),
        )

    def test_bold_cite_span_wins_over_other_years(self):
        cite = "Smith 22, quoting a 1988 study. Accessed 3-25-2025."
        self.assertEqual(
            generate_date_from_cite(cite, emphasized_ranges=[(0, 8)]),
            datetime.date(2022, 1, 1),
        )

    def test_access_date_is_not_evidence_date(self):
        self.assertEqual(
            generate_date_from_cite("Smith, Article title. Accessed 3-25-2025."),
            None,
        )

    def test_implausible_future_year_is_rejected(self):
        self.assertEqual(generate_date_from_cite("Smith 2099, Article"), None)

    def test_malformed_cite_is_unknown(self):
        self.assertEqual(generate_date_from_cite("no reliable date here"), None)


if __name__ == "__main__":
    unittest.main()
