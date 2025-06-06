import os
from typing import Any, KeysView

import pandas as pd
from numpy import ndarray
from pandas.errors import ParserError

from conf.config import LOG_FILE, SQL_SCHEME, config_file


class sql_getError(Exception):
    def __init__(self, col: list[str], all_cols: list[str], *args: object) -> None:
        self.message = f"Not correct get='{col}' argument."
        self.message = f"possible options: {all_cols}"
        super().__init__(*args)

    def __str__(self) -> str:
        return f"SQL get error: {self.message}"


class prepare_tabError(Exception):
    def __init__(self, tab: str, missing_cols: list[str], *args: object) -> None:
        self.message = f"For table {tab} missing mandatory columns: {missing_cols}"
        super().__init__(*args)

    def __str__(self) -> str:
        return f"Write BOM error: {self.message}"


class sql_tabError(Exception):
    def __init__(self, tab: str, tabs: KeysView[str], *args: object) -> None:
        self.message = f"Table '{tab}' is missing or corrupted.\n"
        self.message += f"Available tables are {str(tabs)}"
        super().__init__(*args)

    def __str__(self) -> str:
        return f"SQL table error: {self.message}"


class check_dirError(Exception):
    def __init__(
        self, directory: str, *args: object, file: str = "", scan_dir: str = ""
    ) -> None:
        if not scan_dir and not file:
            self.message = f"{directory} is not existing."
        else:
            self.message = (
                f"{file} is missing or corrupted,\n"
                + "or no {scan_dir} folder in {directory} directory"
            )
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


class scan_dir_permissionError(Exception):
    def __init__(self, directory: str, *args: object) -> None:
        self.message = f"you don't have permission to '{directory}'"
        super().__init__(*args)

    def __str__(self) -> str:
        return f"Permission error: {self.message}"


class read_jsonError(Exception):
    def __init__(self, json_file: str, *args: object) -> None:
        self.message = f"JSON file '{json_file}' is missing or corrupted."
        super().__init__(*args)

    def __str__(self) -> str:
        return f"JSON read error: {self.message}"


class sql_schemeError(Exception):
    """SQL scheme wrong format in json file"""

    def __init__(self, tab: str, *args: object) -> None:
        self.message = f"wrong '{tab}' definition"
        super().__init__(*args)

    def __str__(self) -> str:
        return f"SQL scheme format error: {self.message}. Check {SQL_SCHEME} file."


class sql_executeError(Exception):
    def __init__(self, err: object, cmd: str, *args: object) -> None:
        self.message = str(err) + " on cmd:\n" + cmd[0:100]
        if len(cmd) > 100:
            self.message += "[...]"
        super().__init__(*args)

    def __str__(self) -> str:
        return f"SQL execution error: {self.message}"


class ambigous_matchError(Exception):
    def __init__(self, cmd: str, *args: object, matches: list[str]) -> None:
        self.message = f"Ambiguous abbreviation '{cmd}', match: {matches}."
        super().__init__(*args)

    def __str__(self) -> str:
        return f"match error: {self.message}"


class no_matchError(Exception):
    def __init__(self, cmd: str, *args: object) -> None:
        self.message = f"No match found for abbreviation '{cmd}'."
        super().__init__(*args)

    def __str__(self) -> str:
        return f"match error: {self.message}"


class messageHandler:
    def __init__(self) -> None:
        self.message = []
        # store dataframe hash, to avoid showing twice the same info
        self.df_hash: float = 0

    def SQL_file_miss(self, db_file: str) -> None:
        self.message.append(f"SQL file {db_file} is missing!")
        self.message.append(f"Creating new DB: {db_file}")
        self.__exec__(warning=True)

    def column_miss(self, miss_cols: list[str], file: str, tab: str = "") -> None:
        self.message.append(f"File {file} does not have all necessary columns ")
        if tab != "":
            self.message.append(f"in table {tab}.")
        self.message.append(f"Missing columns: {str(miss_cols)}")
        self.message.append(f"Not importing to table {tab}.")
        self.__exec__(warning=True)

    def import_file(self, file: str) -> None:
        self.message.append("")
        self.message.append("*********************************************************")
        self.message.append("______Import_______")
        self.message.append(f"Importing file: {os.path.basename(file)}")
        self.__exec__()

    def reimport_missing_file(
        self,
        file: str | None = None,
        project: str | None = None,
    ) -> None:
        if not file and not project:
            self.message.append("No not-commited projects to reimport")
        else:
            self.message.append(f" File '{file}' is missing for project '{project}'")
        self.__exec__()

    def import_missing_file(self) -> None:
        self.message.append("No files found to import")
        self.__exec__()

    def export_missing_data(self) -> None:
        self.message.append("No data to export")
        self.__exec__()

    def na_rows(
        self, row_id: list[int] | None = None, rows: pd.DataFrame = pd.DataFrame()
    ) -> None:
        """Warning about NAs in table"""
        if row_id is not None:
            self.message.append(
                f"Missing necessery data in rows: {row_id}. Skiping these rows."
            )
        if not rows.empty:
            df_hash = float(pd.util.hash_pandas_object(rows).sum())  # type: ignore
            if self.df_hash != df_hash:
                self.df_hash = df_hash
                self.message.append("These rows have NAs in NON essential columns:")
                self.message.append(rows)
        self.__exec__(warning=True)

    def file_already_imported(self, file: str) -> bool:
        self.message.append(f"File {file} was already imported.")
        self.message.append("Consider using option --overwrite.")
        self.message.append(
            "Are you sure you want to add this file again (will add to qty.)? (y/n)"
        )
        self.__exec__(warning=True)
        if input() == "y":
            return True
        return False

    def BOM_remove(self, project: list[str]) -> None:
        self.message.append(f"Removed data from BOM table where project == '{project}'")
        self.__exec__()

    def BOM_prepare_projects(
        self,
        project: list[str],
        available: list[str],
        all_projects: list[str],
    ) -> None:
        project_not_available = [
            p for p in project if p not in available and p in all_projects
        ]
        project_not_exist = [p for p in project if p not in all_projects]
        if project_not_available:
            self.message.append(
                f"Project '{project_not_available}' already commited. Skipping."
            )
        if project_not_exist:
            self.message.append(f"No project {project_not_exist} in BOM.")
        if available:
            self.message.append(f"Available projects are: {available}.")
        else:
            self.message.append("No available not-commited projects.")
        self.__exec__()

    def BOM_import_summary(self, dat: pd.DataFrame, ex_devs: int = 0) -> None:
        self.message.append("")
        self.message.append("______SUMMARY_______")
        if dat.empty:
            self.message.append("No devices were added to the table.")
        else:
            if "device_id" in dat.columns:
                new_devs = len(dat) - ex_devs
                self.message.append(f"{new_devs} new devices were added to the table.")
                self.message.append(
                    f"{ex_devs} existing devices were added to the table."
                )
            if "price" in dat.columns:
                if "qty" not in dat.columns:
                    dat["qty"] = 1
                dat["tot_cost"] = dat["price"] * dat["qty"]
                cost = round(dat["tot_cost"].sum(), 2)
                self.message.append(f"{len(dat)} With cost of {cost}$ in total.")
        self.message.append("*********************************************************")
        self.__exec__()

    def BOM_info(self, must_cols: list[str], nice_cols: list[str]) -> None:
        self.message.append("these columns MUST be present in import file:")
        self.message.append(must_cols)
        self.message.append(
            "these columns are optional (but recomended) in import file:"
        )
        self.message.append(nice_cols)
        self.__exec__()

    def msg(self, msg: str) -> None:
        self.message.append(msg)
        self.__exec__()

    def trans_summary(self, txt: list[dict]) -> None:
        self.message.append("")
        self.message.append("*********************************************************")
        self.message.append("______TRANSACTION_______")
        for d in txt:
            if d["shop"]:
                self.message.append(
                    f"Shopping cart for shop '{d['shop']}' saved in '{d['file']}_{d['shop']}' in '{d['dir']}'"
                )
                self.message.append(f"Shopping cart value: '{d['price']}'$")
            else:
                self.message.append(
                    f"Shopping cart saved in '{d['file']}' in '{d['dir']}'"
                )
        self.message.append("*********************************************************")
        self.__exec__()

    def unknown_import(self, er: BaseException) -> None:
        self.message.append(f"Unexpected error: {er}")
        self.message.append("Possibly wrong file format (different shop?) or wrong csv")
        self.__exec__()

    def log_path_error(self, err: str) -> None:
        self.message.append(err)
        self.message.append(f"Provide correct path in '{config_file()}' file.")
        self.__exec__(warning=True)

    def unknown_project(self, project: str, projects: list[str]) -> None:
        self.message.append(f"Unknown project: '{project}'.")
        self.message.append(f"Available project are: {projects}")
        self.__exec__()

    def project_as_filename(self) -> None:
        self.message.append("Set project name as file name.")
        self.message.append("You can change later with admin commands.")
        self.__exec__()

    def __exec__(self, warning: bool = False) -> None:
        if warning:
            print("")
            print("WARNING:")
        for msg in self.message:
            print(msg)
        self.message = []
