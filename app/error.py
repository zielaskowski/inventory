import os
import pandas as pd


class sql_getError(Exception):
    def __init__(
        self, col: list[str], all_cols: list[str], *args: object
    ) -> None:
        self.message = f"Not correct get='{col}' argument."
        self.message = f"possible options: {all_cols}"
        super().__init__(*args)

    def __str__(self) -> str:
        return f"SQL get error: {self.message}"


class prepare_tabError(Exception):
    def __init__(
        self, tab: str, missing_cols: list[str], *args: object
    ) -> None:
        self.message = (
            f"For table {tab} missing mandatory columns: {missing_cols}"
        )
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
    def __init__(
        self, file: str, dir: str, scan_dir: str, *args: object
    ) -> None:
        self.message = f"{file} is missing or corrupted,\nor no {scan_dir} folder in {dir} directory"
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
            self.message += "[...]"
        super().__init__(*args)

    def __str__(self) -> str:
        return f"SQL execution error: {self.message}"


class messageHandler:
    def __init__(self) -> None:
        self.message = []
        # store dataframe hash, to avoid showing twice the same info
        self.df_hash: float = 0

    def SQL_file_miss(self, db_file: str) -> None:
        self.message.append(f"SQL file {db_file} is missing!")
        self.message.append(f"Creating new DB: {db_file}")
        self.__exec__(warning=True)

    def column_miss(
        self, miss_cols: list[str], file: str, tab: str = ""
    ) -> None:
        self.message.append(f"File {file} does not have all necessary columns ")
        if tab != "":
            self.message.append(f"in table {tab}.")
        self.message.append(f"Missing columns: {str(miss_cols)}")
        self.message.append(f"Not importing to table {tab}.")
        self.__exec__(warning=True)

    def import_file(self, file: str) -> None:
        self.message.append("")
        self.message.append(
            "*********************************************************"
        )
        self.message.append("______Import_______")
        self.message.append(f"Importing file: {os.path.basename(file)}")
        self.__exec__()

    def na_rows(
        self, row_id: list[int] = [], rows: pd.DataFrame = pd.DataFrame()
    ) -> None:
        if row_id != []:
            self.message.append(
                f"Missing necessery data in rows: {row_id}. Skiping these rows."
            )
            self.__exec__(warning=True)
        elif not rows.empty:
            df_hash = float(pd.util.hash_pandas_object(rows).sum())
            if self.df_hash != df_hash:
                self.df_hash = df_hash
                self.message.append(
                    "These rows have NAs in NON essential columns:"
                )
                self.message.append(rows.__str__())
                self.__exec__(warning=True)

    def file_already_imported(self, file: str) -> bool:
        self.message.append(f"File {file} was already imported.")
        self.message.append("Consider using option --overwrite.")
        self.message.append(
            "Are you sure you want to add this file again? (y/n)"
        )
        self.__exec__(warning=True)
        if input() == "y":
            return True
        else:
            return False

    def BOM_remove(self, file: str) -> None:
        self.message.append("Removed data from BOM table")
        if file is not None:
            self.message.append(f"where file within {file}")
        self.__exec__()

    def BOM_import_summary(self, dat: pd.DataFrame, ex_devs: int = 0) -> None:
        self.message.append("")
        self.message.append("______SUMMARY_______")
        if dat.empty:
            self.message.append("No devices were added to the table.")
        else:
            if "device_id" in dat.columns:
                new_devs = len(dat) - ex_devs
                self.message.append(
                    f"{new_devs} new devices were added to the table."
                )
                self.message.append(
                    f"{ex_devs} existing devices were added to the table."
                )
            if "price" in dat.columns:
                if "qty" not in dat.columns:
                    dat["qty"] = 1
                dat["tot_cost"] = dat["price"] * dat["qty"]
                cost = round(dat["tot_cost"].sum(), 2)
                self.message.append(
                    f"{len(dat)} With cost of {cost}$ in total."
                )
        self.message.append(
            "*********************************************************"
        )
        self.__exec__()

    def trans_summary(self, txt: list[dict]) -> None:
        self.message.append("")
        self.message.append(
            "*********************************************************"
        )
        self.message.append("______TRANSACTION_______")
        for d in txt:
            if d['shop']:
                self.message.append(
                    f"Shopping cart for shop '{d['shop']}' saved in '{d['file']}_{d['shop']}' in '{d['dir']}'"
                )
                self.message.append(
                    f"Shopping cart value: '{d['price']}'$"
                )
            else:
                self.message.append(
                    f"Shopping cart saved in '{d['file']}' in '{d['dir']}'"
                )
        self.message.append(
            "*********************************************************"
        )
        self.__exec__()

    def unknown_import(self, er: str) -> None:
        self.message.append(f"Unexpected error: {er}")
        self.message.append("Possibly wrong excel format (different shop?)")
        self.__exec__()

    def __exec__(self, warning: bool = False) -> None:
        if warning:
            print("")
            print("WARNING:")
        for msg in self.message:
            print(msg)
        self.message = []
