import logging  # Import logging to provide detailed runtime information.


# Custom exception for invalid operations on HashMap
class HashMapError(Exception):
    """Custom exception for errors related to HashMap operations."""
    pass


class HashMap:
    """
    A custom HashMap implementation that uses an array with chaining to handle collisions.
    Supports basic operations such as get, put, delete, and resizing.
    """

    def __init__(self, initial_capacity: int = 16, load_factor: float = 0.75):
        """
        Initialize the HashMap with a certain capacity and load factor.
        Args:
            initial_capacity: The initial number of buckets (array size).
            load_factor: The threshold at which to resize the map.
        """
        # Validate that load factor is within a valid range (0 < load_factor <= 1)
        if not (0 < load_factor <= 1):
            raise ValueError("Load factor must be between 0 and 1.")

        # Initialize an array (list of lists) to store key-value pairs using chaining for collisions.
        self._buckets = [[] for _ in range(initial_capacity)]

        # Track the current number of elements in the hashmap.
        self._size = 0

        # Set the load factor threshold for when to resize.
        self._load_factor = load_factor

        # Set the initial capacity of the hashmap.
        self._capacity = initial_capacity

        # Configure logging to track actions within the hashmap.
        logging.basicConfig(level=logging.INFO)

    def _hash(self, key) -> int:
        """
        Hash the key to get an index in the bucket array.
        Args:
            key: The key to hash.
        Returns:
            An index in the bucket array.
        """
        try:
            # Calculate hash index using Python's built-in hash function and modulo by capacity.
            return hash(key) % self._capacity
        except TypeError:
            # Log an error if the key is of an unhashable type.
            logging.error(f"Unhashable key type: {type(key)}")
            raise HashMapError(f"Unhashable key type: {type(key)}")

    def put(self, key, value) -> None:
        """
        Insert or update the value associated with the key.
        Args:
            key: The key to associate with the value.
            value: The value to store in the hash map.
        """
        # Raise an error if the key is None, as None is an invalid key.
        if key is None:
            raise ValueError("Key cannot be None.")

        # Hash the key to determine which bucket to store the key-value pair.
        bucket_index = self._hash(key)

        # Retrieve the bucket (list) at the calculated index.
        bucket = self._buckets[bucket_index]

        # Iterate over the bucket to check if the key already exists.
        for i, (k, v) in enumerate(bucket):
            if k == key:
                # Update the value if the key is found and log the action.
                bucket[i] = (key, value)
                logging.info(f"Key '{key}' updated with value '{value}'.")
                return

        # If the key does not exist, append the new key-value pair to the bucket.
        bucket.append((key, value))
        self._size += 1  # Increase the size to reflect the new element.
        logging.info(f"Key '{key}' added with value '{value}'.")

        # If the size exceeds the load factor threshold, resize the hashmap.
        if self._size / self._capacity > self._load_factor:
            self._resize()

    def get(self, key):
        """
        Retrieve the value associated with the key.
        Args:
            key: The key to search in the hash map.
        Returns:
            The value associated with the key, or None if the key is not found.
        """
        # Calculate the bucket index using the hash function.
        bucket_index = self._hash(key)

        # Retrieve the bucket at the index.
        bucket = self._buckets[bucket_index]

        # Iterate through the bucket to find the key-value pair.
        for k, v in bucket:
            if k == key:
                # Log and return the value if the key is found.
                logging.info(f"Key '{key}' found with value '{v}'.")
                return v

        # Log and return None if the key was not found.
        logging.info(f"Key '{key}' not found.")
        return None

    def delete(self, key) -> None:
        """
        Remove the key-value pair from the hash map.
        Args:
            key: The key to remove from the hash map.
        """
        # Calculate the bucket index using the hash function.
        bucket_index = self._hash(key)

        # Retrieve the bucket at the index.
        bucket = self._buckets[bucket_index]

        # Iterate through the bucket to find and remove the key-value pair.
        for i, (k, v) in enumerate(bucket):
            if k == key:
                # Delete the key-value pair and decrease the size.
                del bucket[i]
                self._size -= 1
                logging.info(f"Key '{key}' deleted.")
                return

        # Raise an error if the key is not found.
        raise HashMapError(f"Key '{key}' not found.")

    def contains(self, key) -> bool:
        """
        Check if the key exists in the hash map.
        Args:
            key: The key to search for.
        Returns:
            True if the key exists in the hash map, False otherwise.
        """
        # Return True if the key is found, False otherwise.
        return self.get(key) is not None

    def size(self) -> int:
        """
        Get the number of key-value pairs in the hash map.
        Returns:
            The number of key-value pairs stored in the hash map.
        """
        # Return the current size of the hashmap (number of key-value pairs).
        return self._size

    def _resize(self) -> None:
        """
        Resize the hash map by doubling its capacity and rehashing all existing key-value pairs.
        """
        # Double the capacity of the hash map.
        new_capacity = self._capacity * 2
        logging.info(f"Resizing HashMap to new capacity: {new_capacity}")

        # Create new buckets to hold the rehashed key-value pairs.
        new_buckets = [[] for _ in range(new_capacity)]

        # Rehash existing keys and move them to the new buckets.
        for bucket in self._buckets:
            for key, value in bucket:
                # Calculate the new bucket index based on the new capacity.
                new_bucket_index = hash(key) % new_capacity
                # Insert the key-value pair into the new bucket.
                new_buckets[new_bucket_index].append((key, value))

        # Update the capacity and replace the old buckets with the new ones.
        self._capacity = new_capacity
        self._buckets = new_buckets

    def keys(self) -> list:
        """
        Get a list of all keys stored in the hash map.
        Returns:
            A list of all keys.
        """
        # Create a list of all keys by iterating through all the buckets.
        all_keys = [k for bucket in self._buckets for k, v in bucket]
        logging.info(f"All keys: {all_keys}")
        return all_keys

    def values(self) -> list:
        """
        Get a list of all values stored in the hash map.
        Returns:
            A list of all values.
        """
        # Create a list of all values by iterating through all the buckets.
        all_values = [v for bucket in self._buckets for k, v in bucket]
        logging.info(f"All values: {all_values}")
        return all_values
