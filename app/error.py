"""Error classes"""

from typing import KeysView

from conf.config import SQL_SCHEME


class SqlGetError(Exception):
    """exception class"""

    def __init__(self, col: list[str], all_cols: list[str], *args: object) -> None:
        self.message = f"Not correct get='{col}' argument."
        self.message += f"possible options: {all_cols}"
        super().__init__(*args)

    def __str__(self) -> str:
        return f"SQL get error: {self.message}"


class PrepareTabError(Exception):
    """exception class"""

    def __init__(
        self,
        tab: str,
        file: str,
        missing_cols: list[str],
        *args: object,
    ) -> None:
        self.tab = tab
        self.message = f"File {file} \n"
        self.message += f"does not have all necessary columns in table {tab}.\n"
        self.message += f"Missing columns: {str(missing_cols)}\n"
        self.message += "Did you specify correct file format?\n"
        self.message += f"Not importing to table {tab}."
        super().__init__(*args)

    def __str__(self) -> str:
        return f"Write {self.tab} error: {self.message}"


class SqlTabError(Exception):
    """exception class"""

    def __init__(self, tab: str, tabs: KeysView[str], *args: object) -> None:
        self.message = f"Table '{tab}' is missing or corrupted.\n"
        self.message += f"Available tables are {str(tabs)}"
        super().__init__(*args)

    def __str__(self) -> str:
        return f"SQL table error: {self.message}"


class CheckDirError(Exception):
    """exception class"""

    def __init__(
        self,
        directory: str,
        *args: object,
        file: str = "",
        project: str = "",
        scan_dir: str = "",
    ) -> None:
        if not project:
            self.message = (
                f"{directory} is not existing."
                + f"or no {scan_dir} folder in {directory} directory"
            )
        else:
            self.message = (
                f"For project '{project}', " 
                + f"file '{file}' is missing or corrupted,\n"
        )# fmt: skip
        super().__init__(*args)

    def __str__(self) -> str:
        return f"Check directory error: {self.message}"


class SqlCheckError(Exception):
    """exception class"""

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


class SqlCreateError(Exception):
    """exception class"""

    def __init__(self, sql_scheme: str, *args: object) -> None:
        self.message = f"DB not created. Possibly '{sql_scheme}' file corupted."
        super().__init__(*args)

    def __str__(self) -> str:
        return f"SQL create error: {self.message}"


class ScanDirPermissionError(Exception):
    """exception class"""

    def __init__(self, directory: str, *args: object) -> None:
        self.message = f"you don't have permission to '{directory}'"
        super().__init__(*args)

    def __str__(self) -> str:
        return f"Permission error: {self.message}"


class WriteJsonError(Exception):
    """exception class"""

    def __init__(self, file: str, *args: object) -> None:
        self.message = f"JSON fil '{file}' is missing or data corrupted."
        super().__init__(*args)

    def __str__(self) -> str:
        return f"JSON write error: {self.message}"


class ReadJsonError(Exception):
    """exception class"""

    def __init__(self, json_file: str, *args: object, type_val: str = "") -> None:
        self.message = f"JSON file '{json_file}' is missing or corrupted.\n"
        if type_val is not None:
            self.message += f"Expected '{type_val}' as values"
        super().__init__(*args)

    def __str__(self) -> str:
        return f"JSON read error: {self.message}"


class SqlSchemeError(Exception):
    """SQL scheme wrong format in json file"""

    def __init__(self, tab: str, *args: object) -> None:
        self.message = f"wrong '{tab}' definition"
        super().__init__(*args)

    def __str__(self) -> str:
        return f"SQL scheme format error: {self.message}. Check {SQL_SCHEME} file."


class SqlExecuteError(Exception):
    """exception class"""

    def __init__(self, err: object, cmd: str, *args: object) -> None:
        self.message = str(err) + " on cmd:\n" + cmd[0:100]
        if len(cmd) > 100:
            self.message += "[...]"
        super().__init__(*args)

    def __str__(self) -> str:
        return f"SQL execution error: {self.message}"


class AmbigousMatchError(Exception):
    """exception class"""

    def __init__(self, cmd: str, *args: object, matches: list[str]) -> None:
        self.message = f"Ambiguous abbreviation '{cmd}', match: {matches}."
        super().__init__(*args)

    def __str__(self) -> str:
        return f"match error: {self.message}"


class NoMatchError(Exception):
    """exception class"""

    def __init__(self, cmd: str, *args: object) -> None:
        self.message = f"No match found for abbreviation '{cmd}'."
        super().__init__(*args)

    def __str__(self) -> str:
        return f"match error: {self.message}"
