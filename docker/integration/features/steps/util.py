db_name = "load-test-database"
collection_name = "load-test-collection"


def table_name(counter: int) -> str:
    namespace = f"{db_name}{counter}"
    table = f"{collection_name}{counter}"

    return f"{namespace}:{table}".replace("-", "_").replace(".", "_")
