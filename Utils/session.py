from repoze.lru import LRUCache
from Utils.singleton import Singleton
from Utils.config import RConfig


class RMemorySessionStore(Singleton):
    def __init__(self):
        if hasattr(self, '_init'):
            return
        self._init = True
        self.config = RConfig()
        self._cache = LRUCache(self.config.session_cache_size)

    def push(self, session: str, data):
        self._cache.put(session, data)

    def get(self, session: str):
        return self._cache.get(session, None)

    def remove(self, session: str):
        try:
            self._cache.put(session, None)
        except KeyError:
            pass

    def contains(self, session: str):
        return self._cache.get(session) is not None
