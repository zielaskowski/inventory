import os

class write_bomError(Exception):
    def __init__(self, msg: str, *args: object) -> None:
        self.message = msg
        super().__init__(*args)

    def __str__(self) -> str:
        return f"Write BOM error: {self.message}"


class sql_tabError(Exception):
    def __init__(self, tab: str, tabs: list[str], *args: object) -> None:
        self.message = f"Table '{tab}' is missing or corrupted.\n"
        self.message += f"Available tables are {str(tabs)}"
        super().__init__(*args)

    def __str__(self) -> str:
        return f"SQL table error: {self.message}"


class check_dirError(Exception):
    def __init__(self, message: str, *args: object) -> None:
        self.message = message + " is missing or corrupted."
        super().__init__(*args)

    def __str__(self) -> str:
        return f"Check directory error: {self.message}"


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
        self.message = str(err) + " on cmd:\n" + cmd[0:100]
        if len(cmd) > 100:
            self.message +='[...]'
        super().__init__(*args)

    def __str__(self) -> str:
        return f"SQL execution error: {self.message}"


class hashError(Exception):
    def __init__(self, cols: list[str], *args: object) -> None:
        self.message = f"hash columns {cols} missing in dataFarame"
        super().__init__(*args)

    def __str__(self) -> str:
        return f"Hash error: {self.message}"


class messageHandler:
    def __init__(self) -> None:
        self.message = []

    def SQL_file_miss(self, db_file: str) -> None:
        self.message.append(f"SQL file {db_file} is missing!")
        self.message.append(f"Creating new DB: {db_file}")
        self.__exec__()

    def column_miss(self, miss_cols: list[str], file: str) -> None:
        self.message.append(
            f"File {file} does not have all necessary columns.\n"
            + "Missing columns: "
            + str(miss_cols)
        )
        self.__exec__()

    def import_file(self, file: str) -> None:
        self.message.append("___________________")
        self.message.append("*******************")
        self.message.append(f"Importing file: {os.path.basename(file)}")
        self.__exec__()

    def __exec__(self) -> None:
        for msg in self.message:
            print(msg)
        self.message = []
