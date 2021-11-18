import json

INFO = "INFO: "

def read_configuration_file(config_file):
    """
    Read the json configuration file and
    return a map with the config entries
    """
    with open(config_file, 'r') as json_file:
        configuration = json.load(json_file)
        return configuration