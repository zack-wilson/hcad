import requests

try:
    from cachecontrol import CacheControl
    from cachecontrol.caches.file_cache import FileCache

    http = CacheControl(requests.session(), cache=FileCache(".cache"))
except ImportError:
    http = requests.session()
