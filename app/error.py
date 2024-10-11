class check_dirError(Exception):
    def __init__(self, message: str, *args: object) -> None:
        self.message = message
        super().__init__(*args)

    def __str__(self) -> str:
        return f"Check directory error: {self.message}"

class columnError(Exception):
    def __init__(self, message: str, *args: object) -> None:
        self.message = message
        super().__init__(*args)

    def __str__(self) -> str:
        return f"BOM column error: {self.message}"


class sql_checkError(Exception):
    def __init__(self, db_file: str, tab: str, *args: object) -> None:
        self.message = (
            "Wrong DB scheme in file '"
            + db_file
            + "' or wrong sqlite file."
            + "Problem with table '"
            + tab
            + "'."
        )
        super().__init__(*args)

    def __str__(self) -> str:
        return f"SQL check error: {self.message}"


class sql_createError(Exception):
    def __init__(self, sql_scheme: str, *args: object) -> None:
        self.message = f"DB not created. Possibly '{sql_scheme}' file corupted."
        super().__init__(*args)

    def __str__(self) -> str:
        return f"SQL create error: {self.message}"


class read_jsonError(Exception):
    def __init__(self, json_file: str, *args: object) -> None:
        self.message = f"JSON file '{json_file}' is missing or corrupted."
        super().__init__(*args)

    def __str__(self) -> str:
        return f"JSON read error: {self.message}"


class sql_executeError(Exception):
    def __init__(self, err: object, cmd: str, *args: object) -> None:
        self.message = str(err) + " on cmd: " + cmd
        super().__init__(*args)

    def __str__(self) -> str:
        return f"SQL execution error: {self.message}"


class messageHandler:
    def __init__(self) -> None:
        self.message = []

    def SQL_file_missing(self, db_file: str) -> None:
        self.message.append(f"SQL file {db_file} is missing!")
        self.message.append(f"Creating new DB: {db_file}")
        self.__exec__()

    def __exec__(self) -> None:
        for msg in self.message:
            print(msg)
        self.message = []
