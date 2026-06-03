__all__ = ["DbTools"]


def __getattr__(name):
    if name == "DbTools":
        from .db_tool import DbTools

        return DbTools
    raise AttributeError(name)
