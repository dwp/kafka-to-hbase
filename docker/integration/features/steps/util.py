import logging

db_name = "load-test-database"
collection_name = "load-test-collection"


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = logging.StreamHandler()
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(filename)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)

    return logger


def table_name(counter: int) -> bytes:
    namespace = f"{db_name}{counter}"
    table = f"{collection_name}{counter}"

    # Happybase returns bytes when listing table names so this returns bytes to avoid conversion of the actual data
    return f"{namespace}:{table}".replace("-", "_").replace(".", "_").encode("utf-8")
