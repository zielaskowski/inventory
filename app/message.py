"""class handling messages"""

import os

import pandas as pd

from conf.config import config_file


class messageHandler:
    def __init__(self) -> None:
        self.message = []
        # store dataframe hash, to avoid showing twice the same info
        self.df_hash: float = 0
        self.msg_hash: float = 0

    def SQL_file_miss(self, db_file: str) -> None:
        self.message.append(f"SQL file {db_file} is missing!")
        self.message.append(f"Creating new DB: {db_file}")
        self.__exec__(warning=True)

    def import_file(self, file: str) -> None:
        self.message.append("")
        self.message.append("*********************************************************")
        self.message.append("______Import_______")
        self.message.append(f"Importing file: {os.path.basename(file)}")
        self.__exec__()

    def reimport_missing_file(self) -> None:
        self.message.append("No not-commited projects to reimport")
        self.__exec__()

    def import_missing_file(self) -> None:
        self.message.append("No files found to import")
        self.__exec__()

    def export_missing_data(self) -> None:
        self.message.append("No data to export")
        self.__exec__()

    def na_rows(self, row_id: list[int], rows: pd.DataFrame = pd.DataFrame()) -> None:
        """Warning about NAs in table"""
        if row_id != []:
            self.message.append(
                f"Missing necessery data in rows: {row_id}. Skiping these rows."
            )
        if not rows.empty:
            # df_hash = float(pd.util.hash_pandas_object(rows).sum())  # type: ignore
            # if self.df_hash != df_hash:
            # self.df_hash = df_hash
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

    def __print_col_description__(
        self,
        cols: list[str],
        col_desc: dict[str, str] | dict[str, list[str]],
    ) -> None:
        """print column description if exists"""
        key_len = max(len(str(k)) for k in col_desc.keys())
        for c in cols:
            if c in col_desc:
                self.message.append(f"{c:<{key_len}} : {col_desc[c]}")
            else:
                self.message.append(c + ":-")

    def BOM_info(
        self,
        must_cols: list[str],
        nice_cols: list[str],
        col_desc: dict[str, str],
    ) -> None:
        self.message.append("these columns MUST be present in import file:")
        self.__print_col_description__(cols=must_cols, col_desc=col_desc)
        self.message.append("")
        self.message.append(
            "these columns are optional (but recomended) in import file:"
        )
        self.__print_col_description__(cols=nice_cols, col_desc=col_desc)
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
        msg = [str(s) for s in self.message]  # possibly DataFrame, not only strings
        msg_hash = hash("".join(msg))
        if msg_hash == self.msg_hash or msg == []:
            self.message = []
            return
        self.msg_hash = msg_hash
        if warning:
            print("")
            print("WARNING:")
        for msg in self.message:
            print(msg)
        self.message = []
