import time


def timeit(f):
    def timed(*args, **kw):
        ts = time.time()
        result = f(*args, **kw)
        te = time.time()
        print("func: {} ===> took: {:.4f} sec".format(f.__name__, float(te - ts)))
        return result
    return timed
