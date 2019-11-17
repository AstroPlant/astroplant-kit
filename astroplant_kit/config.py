import toml


def read_config(file):
    """
    Read the configuration file.

    :return: The TOML configuration as a dictionary.
    """
    return toml.load(file)
