import os
from IandO import json_utility as ju


def path_exists(path):
    return os.path.exists(path)


def setup_folders(path):
    try:
        os.makedirs(path, exist_ok=True)
    except FileExistsError:
        pass
