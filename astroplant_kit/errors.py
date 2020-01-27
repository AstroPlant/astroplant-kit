class NoConfigurationError(Exception):
    """
    No configuration set error.
    """

    def __init__(self):
        super().__init__("no configuration has been set")
