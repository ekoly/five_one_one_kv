class ServerError(Exception):
    """
    Server messed up.
    """

    pass


class ClientError(Exception):
    """
    Client messed up.
    """

    pass


class NotEnoughDataError(Exception):
    """
    Need to read more data before parsing.
    """

    pass


class EmbeddedCollectionError(Exception):
    """
    The user tried to imbed a collection type inside another collection.
    """

    pass


class NotHashableError(Exception):
    """
    The user tried to use a key that is not hashable.
    """

    pass
