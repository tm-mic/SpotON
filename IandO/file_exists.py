import os


def path_exists(path):
    """
    Check if a path exists in the directory.

    :param path: Path in directory.
    :return: Bool if path exists.
    """
    return os.path.exists(path)


def setup_folders(path):
    """
    Creates a folder at the given path. If folder already exists pass.

    :param path:
    :return: None
    """
    try:
        os.makedirs(path, exist_ok=True)
    except FileExistsError:
        pass
    return


def files_exists(path):
    """
    Check if a file exists at a given path. If not asks the user to add the file at the provided path.
    Re-runs until a file is provided at the given path.

    :param path: Path to file.
    :return: None
    """
    c = 0
    while c == 0:
        if path_exists(path):
            c = 1
        else:
            c = 0
            print(f"The corresponding file to the given filepath {path} does not exist. Please provide the correct"
                  f" file at the path given in the config.json.")
            input("After you added the file enter any key to continue.")
    return
