#file check module

from os.path import exists

def read_file(path : str):
    print(path)

def validate_file_path(path:str):
    file_exists = exists(path)

