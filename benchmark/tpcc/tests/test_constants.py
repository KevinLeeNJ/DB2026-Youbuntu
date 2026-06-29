import unittest

from benchmark.tpcc.core.constants import C_LAST_SYLLABLES, nurand, surname


class ConstantsTest(unittest.TestCase):
    def test_surname_known_values(self) -> None:
        self.assertEqual(surname(0), "BARBARBAR")
        self.assertEqual(surname(255), "ABLEESEESE")
        self.assertEqual(surname(999), "EINGEINGEING")

    def test_nurand_stays_in_bounds(self) -> None:
        values = [nurand(255, 0, 999) for _ in range(1000)]
        self.assertTrue(all(0 <= value <= 999 for value in values))

    def test_syllable_table_has_ten_entries(self) -> None:
        self.assertEqual(len(C_LAST_SYLLABLES), 10)


if __name__ == "__main__":
    unittest.main()
