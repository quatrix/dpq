import hashlib

class RedisLua:
    def __init__(self, redis):
        self.redis = redis
        self.load_source(self.get_lua_path())
        self.register()

    def load_source(self, path):
        self.source = open(path).read()

    def register(self):
        self.sha1 = self.get_source_sha1()

        if not self.exists():
            self.load()

    def get_lua_path(self):
        import dpq
        import pkg_resources

        return pkg_resources.resource_filename('dpq', 'redis-priority-queue.lua')

    def get_source_sha1(self):
        d = hashlib.sha1(self.source.encode('utf-8'))
        d.digest()
        return d.hexdigest()

    def exists(self):
        t = self.redis.script_exists(self.sha1)

        if t and t[0]:
            return True

        return False

    def load(self):
        self.redis.script_load(self.source)

    def eval(self, *args):
        return self.redis.evalsha(self.sha1, 0, *args)
