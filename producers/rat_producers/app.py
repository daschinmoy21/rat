class App:
    def __init__(self):
        self.manifests = {}   # name -> plugin.toml dict
        self.sources = {}     # name -> poll fn
        self.extractors = {}  # name -> fn
        self.schemas = {}     # name -> payload validator (or None)

    def add_source(self, name, poll):
        self.sources[name] = poll

    def add_extractor(self, name, fn):
        self.extractors[name] = fn
