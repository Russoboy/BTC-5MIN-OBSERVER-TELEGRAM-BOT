"""Small dependency-free checks for the event rules."""
import unittest
from rules import (
    is_opposite_80,
    is_pullback,
    is_sweet_spot,
    remaining_label,
    sweet_spot_category,
    switch_up_result,
)


class EventRuleTests(unittest.TestCase):
    def test_sweet_spot_is_strictly_over_80(self):
        self.assertFalse(is_sweet_spot(0.80))
        self.assertTrue(is_sweet_spot(0.8001))

    def test_pullback_is_initial_side_at_or_below_55(self):
        self.assertTrue(is_pullback("UP", "UP", 0.55))
        self.assertFalse(is_pullback("UP", "DOWN", 0.30))
        self.assertFalse(is_pullback("UP", "UP", 0.56))

    def test_opposite_side_80(self):
        self.assertTrue(is_opposite_80("DOWN", "UP", 0.80))
        self.assertFalse(is_opposite_80("UP", "UP", 0.90))

    def test_switch_up_is_resolution_dependent(self):
        self.assertEqual(switch_up_result("UP", True, "DOWN"), "YES")
        self.assertEqual(switch_up_result("UP", True, "UP"), "NO")
        self.assertEqual(switch_up_result("UP", False, "DOWN"), "NO")
        self.assertIsNone(switch_up_result("UP", True, None))

    def test_time_category(self):
        end = 300.0
        self.assertEqual(remaining_label(end, 42), "[4, 18]")
        self.assertEqual(sweet_spot_category(end, 42), "3 min+")
        self.assertEqual(sweet_spot_category(end, 120), "[3, 00]")
        self.assertEqual(sweet_spot_category(end, 119), "3 min+")
        self.assertEqual(sweet_spot_category(end, 121), "[2, 59]")


if __name__ == "__main__":
    unittest.main()
