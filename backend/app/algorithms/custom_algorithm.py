"""
Custom algorithms for the Urban Mobility Data Explorer backend.

Had to implement these manually since we can't use built-in sorting/grouping.
Here's what we've got:

1. Merge sort - for ranking zones by revenue
2. Top-K selection (quickselect) - finding top pickup hours efficiently
3. Custom hash map - grouping trips without SQL GROUP BY
4. Anomaly detection - finding weird trips using Z-scores

Written by Modupe Akanni
"""

from typing import List, Dict, Any, Optional, Tuple


# Merge sort for ranking zones by revenue

def _merge(left: List[Dict], right: List[Dict], key: str) -> List[Dict]:
    """
    Merges two sorted lists into one. Standard merge step from merge sort.
    We're sorting in descending order (highest first).
    
    Time: O(n), Space: O(n)
    """
    result = []
    i = 0
    j = 0

    while i < len(left) and j < len(right):
        if left[i][key] >= right[j][key]:
            result.append(left[i])
            i += 1
        else:
            result.append(right[j])
            j += 1

    while i < len(left):
        result.append(left[i])
        i += 1

    while j < len(right):
        result.append(right[j])
        j += 1

    return result


def merge_sort(data: List[Dict], key: str) -> List[Dict]:
    """
    Classic merge sort implementation. Recursively splits the list in half,
    sorts each half, then merges them back together.
    
    Returns data sorted by the given key in descending order.
    
    Time: O(n log n), Space: O(n)
    """
    if len(data) <= 1:
        return data

    mid = len(data) // 2
    left = merge_sort(data[:mid], key)
    right = merge_sort(data[mid:], key)

    return _merge(left, right, key)


def rank_zones_by_revenue(zone_revenue_list: List[Dict]) -> List[Dict]:
    """
    Sorts zones by revenue using our merge sort. Used by the top revenue zones endpoint.
    Returns zones with highest revenue first.
    """
    return merge_sort(zone_revenue_list, key="total_revenue")


# Top-K selection using quickselect - more efficient than sorting everything

def _partition(arr: List[Dict], low: int, high: int, key: str) -> int:
    """
    Partition step for quickselect. Picks the last element as pivot and
    rearranges so elements >= pivot are on the left.
    
    Returns the final position of the pivot.
    Time: O(n), Space: O(1)
    """
    pivot_value = arr[high][key]
    i = low - 1

    for j in range(low, high):
        if arr[j][key] >= pivot_value:
            i += 1
            arr[i], arr[j] = arr[j], arr[i]

    arr[i + 1], arr[high] = arr[high], arr[i + 1]
    return i + 1


def _quickselect(arr: List[Dict], low: int, high: int, k: int, key: str) -> None:
    """
    Quickselect - finds the k-th largest element without fully sorting.
    Modifies the array in-place so the k largest elements end up at positions 0..k-1.
    
    Time: O(n) average, O(n²) worst case (rare though)
    """
    if low < high:
        pivot_index = _partition(arr, low, high, key)

        if pivot_index == k:
            return
        elif pivot_index > k:
            _quickselect(arr, low, pivot_index - 1, k, key)
        else:
            _quickselect(arr, pivot_index + 1, high, k, key)


def top_k_selection(data: List[Dict], k: int, key: str) -> List[Dict]:
    """
    Gets the top k elements without sorting everything. Uses quickselect to partition,
    then sorts just the top k. Much faster when k is small compared to n.
    
    Time: O(n + k log k) - way better than O(n log n) when k << n
    """
    if k >= len(data):
        return merge_sort(data, key)
    
    if k <= 0:
        return []
    
    # Make a copy so we don't mess with the original
    arr = [item.copy() for item in data]
    
    # Partition so k largest are at the front
    _quickselect(arr, 0, len(arr) - 1, k - 1, key)
    
    # Sort just the top k for final ordering
    top_k = arr[:k]
    return merge_sort(top_k, key)


def get_top_pickup_hours(hourly_data: List[Dict], top_n: int = 5) -> List[Dict]:
    """
    Finds the busiest pickup hours. Uses top-k selection so we don't have to
    sort all 24 hours if we only want the top 5.
    """
    return top_k_selection(hourly_data, top_n, key="trip_count")


# ============================================================================
# 3. CUSTOM HASH MAP - For Grouping Trips
# ============================================================================

class CustomHashMap:
    """
    Custom hash map implementation built from scratch for grouping operations.
    Uses chaining for collision resolution.
    
    Pseudocode:
        CLASS CustomHashMap:
            INITIALIZE(buckets=16):
                self.buckets = ARRAY of buckets
                self.size = 0
            
            HASH(key):
                hash_value = 0
                FOR each char in str(key):
                    hash_value = (hash_value * 31 + ord(char)) MOD buckets
                RETURN hash_value
            
            PUT(key, value):
                index = HASH(key)
                bucket = self.buckets[index]
                
                FOR each entry in bucket:
                    IF entry.key == key:
                        entry.value = value
                        RETURN
                
                bucket.append({key: key, value: value})
                self.size++
            
            GET(key):
                index = HASH(key)
                bucket = self.buckets[index]
                
                FOR each entry in bucket:
                    IF entry.key == key:
                        RETURN entry.value
                
                RETURN None
            
            GET_ALL_ENTRIES():
                result = []
                FOR each bucket in self.buckets:
                    FOR each entry in bucket:
                        result.append(entry)
                RETURN result
    
    Time Complexity:
        - PUT: O(1) average case, O(n) worst case (all collisions)
        - GET: O(1) average case, O(n) worst case
        - GET_ALL_ENTRIES: O(n) where n = number of entries
    Space Complexity: O(n) where n = number of key-value pairs
    """
    
    def __init__(self, buckets: int = 16):
        """Initialize hash map with specified number of buckets."""
        self.buckets = [[] for _ in range(buckets)]
        self.size = 0
    
    def _hash(self, key: Any) -> int:
        """Compute hash value for a key."""
        key_str = str(key)
        hash_value = 0
        for char in key_str:
            hash_value = (hash_value * 31 + ord(char)) % len(self.buckets)
        return hash_value
    
    def put(self, key: Any, value: Any) -> None:
        """Insert or update a key-value pair."""
        index = self._hash(key)
        bucket = self.buckets[index]
        
        # Check if key already exists
        for entry in bucket:
            if entry['key'] == key:
                entry['value'] = value
                return
        
        # Add new entry
        bucket.append({'key': key, 'value': value})
        self.size += 1
    
    def get(self, key: Any) -> Optional[Any]:
        """Get value for a key, returns None if not found."""
        index = self._hash(key)
        bucket = self.buckets[index]
        
        for entry in bucket:
            if entry['key'] == key:
                return entry['value']
        
        return None
    
    def contains(self, key: Any) -> bool:
        """Check if key exists in hash map."""
        return self.get(key) is not None
    
    def get_all_entries(self) -> List[Dict]:
        """Get all key-value pairs as list of dictionaries."""
        result = []
        for bucket in self.buckets:
            for entry in bucket:
                result.append(entry)
        return result
    
    def keys(self) -> List[Any]:
        """Get all keys."""
        result = []
        for bucket in self.buckets:
            for entry in bucket:
                result.append(entry['key'])
        return result


def group_trips_by_key(trips: List[Dict], group_key: str, 
                       aggregate_key: str = None, 
                       operation: str = 'count') -> List[Dict]:
    """
    Groups trips by a key using our custom hash map. Can also aggregate values
    like summing up revenue per zone.
    
    Time: O(n), Space: O(m) where m = unique groups
    """
    hash_map = CustomHashMap(buckets=32)
    
    for trip in trips:
        key_value = trip.get(group_key)
        if key_value is None:
            continue
        
        if hash_map.contains(key_value):
            group = hash_map.get(key_value)
            group['count'] += 1
            if aggregate_key and operation == 'sum':
                group['sum'] += trip.get(aggregate_key, 0)
        else:
            group = {
                group_key: key_value,
                'count': 1
            }
            if aggregate_key and operation == 'sum':
                group['sum'] = trip.get(aggregate_key, 0)
            hash_map.put(key_value, group)
    
    result = []
    for entry in hash_map.get_all_entries():
        group = entry['value'].copy()
        if aggregate_key and operation == 'sum' and group['count'] > 0:
            group['avg'] = group['sum'] / group['count']
        result.append(group)
    
    return result


# Anomaly detection using Z-scores - finding trips that are way outside the norm

def calculate_mean(data: List[float]) -> float:
    """
    Calculate the mean manually. Simple sum and divide.
    Time: O(n), Space: O(1)
    """
    if not data:
        return 0.0
    
    total = 0.0
    count = 0
    for value in data:
        total += value
        count += 1
    
    return total / count if count > 0 else 0.0


def calculate_std_dev(data: List[float], mean: float) -> float:
    """
    Calculate standard deviation. First get variance, then take square root.
    Using Newton's method for sqrt since we can't use math.sqrt.
    
    Time: O(n), Space: O(1)
    """
    if len(data) < 2:
        return 0.0
    
    # Calculate variance
    variance_sum = 0.0
    for value in data:
        diff = value - mean
        variance_sum += diff * diff
    
    variance = variance_sum / (len(data) - 1)
    
    # Square root using Newton's method
    if variance == 0:
        return 0.0
    
    x = variance
    for _ in range(10):  # Usually converges pretty fast
        x = 0.5 * (x + variance / x)
    
    return x


def detect_anomalies(trips: List[Dict], 
                    field: str, 
                    threshold: float = 3.0) -> List[Dict]:
    """
    Finds trips that are way outside the normal range using Z-scores.
    A Z-score tells us how many standard deviations away from the mean something is.
    Default threshold of 3.0 means we're looking for trips more than 3 std devs away.
    
    Time: O(n), Space: O(n)
    """
    # Pull out all the values for this field
    values = []
    for trip in trips:
        value = trip.get(field)
        if value is not None:
            values.append(float(value))
    
    if len(values) < 2:
        return []
    
    # Calculate mean and standard deviation
    mean = calculate_mean(values)
    std_dev = calculate_std_dev(values, mean)
    
    if std_dev == 0:
        return []  # Can't detect anomalies if everything is the same
    
    # Find trips with high Z-scores
    anomalies = []
    for trip in trips:
        value = trip.get(field)
        if value is not None:
            z_score = abs((float(value) - mean) / std_dev)
            
            if z_score > threshold:
                anomaly = trip.copy()
                anomaly['z_score'] = z_score
                anomaly['field_analyzed'] = field
                anomaly['mean'] = mean
                anomaly['std_dev'] = std_dev
                anomalies.append(anomaly)
    
    return anomalies


def detect_multiple_anomalies(trips: List[Dict], 
                              fields: List[str], 
                              threshold: float = 3.0) -> List[Dict]:
    """
    Checks multiple fields and finds trips that are anomalous in any of them.
    Useful for finding trips that are weird in fare, distance, duration, etc.
    
    Time: O(n * m) where m = number of fields
    """
    anomaly_map = CustomHashMap()
    
    for field in fields:
        anomalies = detect_anomalies(trips, field, threshold)
        for anomaly in anomalies:
            trip_id = anomaly.get('trip_id') or id(anomaly)
            
            if anomaly_map.contains(trip_id):
                existing = anomaly_map.get(trip_id)
                existing['anomalous_fields'].append({
                    'field': field,
                    'z_score': anomaly['z_score'],
                    'value': anomaly.get(field)
                })
            else:
                anomaly_map.put(trip_id, {
                    'trip': anomaly,
                    'anomalous_fields': [{
                        'field': field,
                        'z_score': anomaly['z_score'],
                        'value': anomaly.get(field)
                    }]
                })
    
    result = []
    for entry in anomaly_map.get_all_entries():
        result.append(entry['value'])
    
    return result
