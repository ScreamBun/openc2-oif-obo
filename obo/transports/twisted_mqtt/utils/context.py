

class DummyLock:
    """
    Not intended to do anything
    """
    def __init__(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        pass
