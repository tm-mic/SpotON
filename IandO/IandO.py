# Responsible for Module: Christian L.; written with support by Max, Thomas
# I/O Module is the basic module to import csv files into a dataframe dictionary
import pandas


def __read_csv_import_config(file_path: str) -> str:
    """open file based on file path.
    Shouldn't be used by itself - no error handling."""
    config_base = open(file_path, mode="r", encoding="UTF-8").read()
    return config_base


def __handle_path_error_for_config(config_file_path: str) -> str:
    """Error Handler for file opener."""
    try:
        config_stream = __read_csv_import_config(config_file_path)
        return config_stream
    except FileNotFoundError:
        print("The path given does not return a config file. Please provide a different path.")


def pull_path_column_list_in_config(config_file_path: str, file_identifier: str) -> tuple:
    """Parsing config file returning file path and column headers based on file identifier given.
    Shouldn't be used by itself."""
    config_stream = __handle_path_error_for_config(config_file_path)
    lines = config_stream.splitlines(keepends=False)
    for i, item in enumerate(lines):
        if item.__contains__(file_identifier):
            return [lines[i + 1], lines[i + 2], lines[i + 3]]
        else:
            pass
    return 0


def handle_value_error_for_single_csv_import(config_tuple: tuple, list_of_headers_from_configfile: object, nrows=1) -> object:
    """If no columns are specified in config file return all columns in dataframe - avoid value error."""
    try:
        if nrows == 1:
            return pandas.read_csv(config_tuple[0], sep=None, header=0, usecols=list_of_headers_from_configfile,
                                   engine='python', encoding_errors='replace', on_bad_lines="skip")
        else:
            return pandas.read_csv(config_tuple[0], sep=None, header=0, usecols=list_of_headers_from_configfile,
                                   engine='python', encoding_errors='replace', on_bad_lines="skip", nrows=nrows)
    except ValueError:
        return pandas.read_csv(config_tuple[0], sep=None, header=0, engine='python', encoding_errors='replace',
                               on_bad_lines="skip", nrows=nrows)


def import_single_file_as_UTF(config_file_path: str, file_identifier: str, nrows=1):
    """Import a single csv file to pandas dataframe with error handling.
    Shouldn't be used by itself."""
    config_tuple = (pull_path_column_list_in_config(config_file_path, file_identifier))
    # TODO: handle if config columns have been written with ',' and not with ';'
    list_of_headers_from_configfile = config_tuple[1].split(sep=';')
    return handle_value_error_for_single_csv_import(config_tuple, list_of_headers_from_configfile, nrows)


def import_all_config_specified_csv(file_list: str, config_file_path: str, nrows=1) -> dict:
    """Main function to be called in this module importing all in the fileconfig.txt specified csv files to a dict.
    \nInput:
    (1)list[] of file identifiers as specified in fileconfig. Column headers need to be specified!
    Otherwise error will be thrown.
    \n(2) File_path to config file.
    (3) Number of rows to be imported 1 = import all rows != 1 import n rows
    \nOutput:
    \n(1) Dictionary containing all imported csv as key = file identifier, value = dataframe from csv"""
    dataframe_dictionary_of_imported_csv = {}
    try:
        for x in file_list:
            imported_csv = import_single_file_as_UTF(config_file_path, x, nrows)
            dataframe_dictionary_of_imported_csv.__setitem__(x, imported_csv)
    except TypeError:
        print(f'Error: The file "{x}" you tried to import does not exist, '
              f'or is not listed in your filepath.txt.\n')
        pass
    return dataframe_dictionary_of_imported_csv
