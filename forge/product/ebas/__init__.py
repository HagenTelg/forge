def is_available() -> bool:
    try:
        from ebas.io.file import nasa_ames
        from nilutility.datatypes import DataObject
    except ImportError:
        return False
    return True
