import os
import pickle
import pandas as pd

class PersistentCache:
    """
    Simple persistent cache for pandas DataFrame or any picklable Python object.
    Load from cache file if available, or run initialization, and store for future sessions.
    """

    def __init__(self, cache_path, initializer_func, *init_args, **init_kwargs):
        """
        cache_path: path to cache file (e.g., 'mapping_cache.pkl')
        initializer_func: function that returns the initialized object (e.g., lambda: pd.read_excel(...))
        *init_args, **init_kwargs: arguments passed to initializer_func if cache does not exist
        """
        self.cache_path = cache_path
        self.initializer_func = initializer_func
        self.init_args = init_args
        self.init_kwargs = init_kwargs
        self._obj = None

    def load(self, force_reload=False):
        """
        Loads the object from cache if available, otherwise runs initializer_func.
        Set force_reload=True to ignore cache and reinitialize.
        """
        if not force_reload and os.path.exists(self.cache_path):
            with open(self.cache_path, 'rb') as f:
                self._obj = pickle.load(f)
            print(f"Loaded from cache: {self.cache_path}")
        else:
            print("Initializing from source...")
            self._obj = self.initializer_func(*self.init_args, **self.init_kwargs)
            with open(self.cache_path, 'wb') as f:
                pickle.dump(self._obj, f)
            print(f"Saved to cache: {self.cache_path}")
        return self._obj

    def clear(self):
        """
        Delete the cache file.
        """
        if os.path.exists(self.cache_path):
            os.remove(self.cache_path)
            print(f"Cache cleared: {self.cache_path}")
        else:
            print("No cache file to clear.")

# --- Example Usage ---

# Define the initializer for your mapping DataFrame
def load_mapping():
    # Replace with your actual mapping file path
    return pd.read_excel('Mapping-DE-DT.xlsx')

# Create persistent cache instance
mapping_cache = PersistentCache(
    cache_path='mapping_cache.pkl',
    initializer_func=load_mapping
)

# To load (will use cache if available)
df_map = mapping_cache.load()

# If you want to force a refresh from source:
# df_map = mapping_cache.load(force_reload=True)

# To manually clear the cache:
# mapping_cache.clear()