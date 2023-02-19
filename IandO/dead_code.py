import json


def write_dict_to_json(dictionary: dict, filepath: str, append=False):
    """Writes dict type to json file. If file does not exist new files is created."""
    if append == 'a':
        write = 'a'
    else:
        write = 'w'
    with open(filepath, write) as outfile:
        json.dump(dictionary, outfile)
    return None
