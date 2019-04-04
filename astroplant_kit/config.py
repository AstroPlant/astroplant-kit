import toml

def read_config():
    """
    Read the configuration file at './kit_config.toml'.

    :return: The TOML configuration as a dictionary.
    """
    with open('./kit_config.toml') as f:
        data = toml.load(f)

    return data
