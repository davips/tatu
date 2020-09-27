from aiuna.compression import pack, unpack


def save(filename, obj):
    """Compress and save a python object (Data, Step) as a file."""
    f = open(filename, "wb")
    f.write(pack(obj))
    f.close()


def load(filename):
    """Load a compressed python object (Data, Step) from file."""
    f = open(filename, "rb")
    res = unpack(f.read())
    f.close()
    return res


def save_txt(filename, text):
    """Save text as a file."""
    f = open(filename, "w")
    f.write(text)
    f.close()
