from __future__ import annotations

import random

DISTRICTS_PER_WAREHOUSE = 10
CUSTOMERS_PER_DISTRICT = 3000
ITEM_COUNT = 100000
INITIAL_ORDERS_PER_DISTRICT = 3000
INITIAL_NEW_ORDERS_PER_DISTRICT = 900

TXN_MIX = {
    "new_order": 45,
    "payment": 43,
    "order_status": 4,
    "delivery": 4,
    "stock_level": 4,
}

C_LAST_SYLLABLES = [
    "BAR",
    "OUGHT",
    "ABLE",
    "PRI",
    "PRES",
    "ESE",
    "ANTI",
    "CALLY",
    "ATION",
    "EING",
]


def rand_int(low: int, high: int) -> int:
    return random.randint(low, high)


def nurand(a: int, x: int, y: int) -> int:
    return ((rand_int(0, a) | rand_int(x, y)) % (y - x + 1)) + x


def surname(number: int) -> str:
    hundreds = number // 100
    tens = (number // 10) % 10
    ones = number % 10
    return C_LAST_SYLLABLES[hundreds] + C_LAST_SYLLABLES[tens] + C_LAST_SYLLABLES[ones]

