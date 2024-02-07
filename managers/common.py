import functools

def requires_connection(func):
    """Decorator to ensure a vCenter connection is established before calling the method."""
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if not self.connection:
            print("Not connected to vCenter. Please establish a connection first.")
            return None
        return func(self, *args, **kwargs)
    return wrapper

def auto_requires_connection(cls):
    for attr_name, attr_value in cls.__dict__.items():
        if callable(attr_value) and not attr_name.startswith("__"):
            setattr(cls, attr_name, requires_connection(attr_value))
    return cls