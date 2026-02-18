"""
app/algorithms/custom_algorithm.py
------------------------------------
Manual Merge Sort implementation — Team Member 4 (Modupe Akanni).

Placed inside the backend package so the analytics service can import it.
TM4 owns the algorithm logic; TM3 (backend) owns the integration wiring.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PROBLEM STATEMENT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Given a list of zone revenue records fetched from the database (unsorted),
rank them from highest total_revenue to lowest WITHOUT using:
  - Python built-in sort() or sorted()
  - pandas sort_values()
  - heapq or any heap library
  - Any third-party sorting utility

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ALGORITHM CHOSEN: Merge Sort
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Merge sort is ideal here because:
  1. Guaranteed O(n log n) in ALL cases — no worst-case O(n²) like quick sort.
  2. Stable sort — equal revenues keep their original relative order.
  3. Clean recursive structure that is easy to trace and explain.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PSEUDOCODE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
merge_sort(data, key):
    if len(data) <= 1:
        return data                         ← base case: already sorted
    mid   = len(data) // 2
    left  = merge_sort(data[:mid],  key)   ← sort left half
    right = merge_sort(data[mid:], key)    ← sort right half
    return merge(left, right, key)          ← merge two sorted halves

merge(left, right, key):
    result = []
    i, j = 0, 0
    while i < len(left) and j < len(right):
        if left[i][key] >= right[j][key]:   ← descending order
            result.append(left[i]); i++
        else:
            result.append(right[j]); j++
    result += remaining elements from whichever list is not exhausted
    return result

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
COMPLEXITY ANALYSIS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Time complexity:
  - The list is halved at each recursion level → log₂(n) levels.
  - merge() scans every element exactly once per level → O(n) per level.
  - Total: O(n log n) — best, average, AND worst case.

Space complexity:
  - Each merge() call creates a new result list of size ≤ n → O(n) auxiliary.
  - The call stack is O(log n) deep.
  - Total auxiliary space: O(n).
"""

from typing import List, Dict


def _merge(left: List[Dict], right: List[Dict], key: str) -> List[Dict]:
    """
    Merge two already-sorted sublists into one sorted list.
    Sorted in DESCENDING order of the numeric field `key`.

    Parameters
    ----------
    left, right : lists of dicts, each already sorted by `key` descending.
    key         : the dict key to compare on (e.g. "total_revenue").

    Returns
    -------
    A new list containing every element from left + right, sorted descending.
    """
    result = []
    i = 0   # pointer into left
    j = 0   # pointer into right

    # Compare the front of each list; take the larger value (descending sort)
    while i < len(left) and j < len(right):
        if left[i][key] >= right[j][key]:
            result.append(left[i])
            i += 1
        else:
            result.append(right[j])
            j += 1

    # One list is exhausted — append whatever remains from the other
    while i < len(left):
        result.append(left[i])
        i += 1

    while j < len(right):
        result.append(right[j])
        j += 1

    return result


def merge_sort(data: List[Dict], key: str) -> List[Dict]:
    """
    Recursively sort `data` in descending order of `data[i][key]`.

    Parameters
    ----------
    data : list of dicts  e.g. [{"zone_name": "JFK", "total_revenue": 9823.5}, ...]
    key  : the numeric field name to sort on.

    Returns
    -------
    A NEW list sorted descending. The original list is NOT modified.
    """
    # Base case: a list of 0 or 1 elements is already sorted
    if len(data) <= 1:
        return data

    mid   = len(data) // 2
    left  = merge_sort(data[:mid],  key)   # sort left half recursively
    right = merge_sort(data[mid:], key)    # sort right half recursively

    return _merge(left, right, key)         # merge the two sorted halves


# ------------------------------------------------------------------ #
# Public entry point — called by analytics_service.py                #
# ------------------------------------------------------------------ #

def rank_zones_by_revenue(zone_revenue_list: List[Dict]) -> List[Dict]:
    """
    Sort a list of zone revenue records by total_revenue (descending)
    using the manual merge sort. No built-in sort is used anywhere.

    Called by:
        analytics_service.get_top_revenue_zones()

    Example input:
        [
            {"zone_name": "JFK Airport",          "total_revenue": 48230.75, ...},
            {"zone_name": "Upper East Side North", "total_revenue": 91042.10, ...},
        ]

    Example output (sorted):
        [
            {"zone_name": "Upper East Side North", "total_revenue": 91042.10, ...},
            {"zone_name": "JFK Airport",           "total_revenue": 48230.75, ...},
        ]
    """
    return merge_sort(zone_revenue_list, key="total_revenue")