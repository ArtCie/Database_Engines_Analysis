import threading
from time import time


def measure_time(f):
    def wrapper(*args):
        method_name = f"{str(args[0].__class__).split('.')[-1][:-2]}.{f.__name__}"
        start = time()
        f(*args)
        print(f"{method_name} Executed in: {time() - start}")
    return wrapper


