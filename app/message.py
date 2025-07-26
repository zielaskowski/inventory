"""class handling messages"""

import os

import pandas as pd

from conf.config import *  # pylint: disable=unused-wildcard-import,wildcard-import


class MessageHandler:  # pylint: disable=too-many-public-methods
    """class methods for simplyfy messagning"""

    def __init__(self) -> None:
        self.message = []
        # store dataframe hash, to avoid showing twice the same info
        self.df_hash: float = 0
        self.msg_hash: float = 0

    def sql_file_miss(self, db_file: str) -> None:
        """message method"""
        self.message.append(f"SQL file {db_file} is missing!")
        self.message.append(f"Creating new DB: {db_file}")
        self.__exec__(warning=True)

    def import_file(self, file: str) -> None:
        """message method"""
        self.message.append("")
        self.message.append("*********************************************************")
        self.message.append("______Import_______")
        self.message.append(f"Importing file: {os.path.basename(file)}")
        self.__exec__()

    def reimport_missing_file(self) -> None:
        """message method"""
        self.message.append("No not-commited projects to reimport")
        self.__exec__()

    def import_missing_file(self) -> None:
        """message method"""
        self.message.append("No files found to import")
        self.__exec__()

    def export_missing_data(self) -> None:
        """message method"""
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

    def project_already_imported(self, project: str) -> bool:
        """message method"""
        if DEBUG in ["pytest", "debugpy"]:
            return True
        self.message.append(f"Project '{project}' was already imported.")
        self.message.append("Consider using option --overwrite.")
        self.message.append(
            "Are you sure you want to add this file again (will add to qty.)? (y/n)"
        )
        self.__exec__(warning=True)
        if input().lower() == "y":
            return True
        return False

    def data_already_imported(self, dat: pd.DataFrame) -> bool:
        """message method"""
        if DEBUG in ["pytest", "debugpy"]:
            return True
        self.message.append("These devices were already imported:")
        self.message.append(dat.loc[:, [DEV_ID, DEV_MAN]].to_string())
        self.message.append("Consider using option --overwrite.")
        self.message.append(
            "Are you sure you want to add this file again (will add to stock_qty.)? (y/n)"
        )
        self.__exec__(warning=True)
        if input().lower() == "y":
            return True
        return False

    def inform_alternatives(self, alternatives: pd.DataFrame) -> bool:
        """found manufacturer alternatives. Do you accept?"""
        if DEBUG in ["pytest", "debugpy"]:
            return True
        self.message.append("Found manufacturer alternative names for incoming data")
        self.message.append(f"Manufacturer alternatives are defined in '{MAN_ALT}'")
        self.message.append(alternatives.drop_duplicates().to_string())
        self.message.append("Do you accept? (y/n)")
        self.__exec__()
        if input().lower() == "y":
            return True
        return False

    def inform_duplications(self, dup: pd.DataFrame) -> None:
        """inform about possible manufacturer duplications"""
        self.message.append(
            "Found devices with different manufacturer, possibly duplication."
        )
        self.message.append(dup.to_string())
        self.message.append(
            "You can align later with 'admin --align_manufacturers' fucntion"
        )
        self.__exec__()

    def bom_remove(self, project: list[str]) -> None:
        """message method"""
        self.message.append(f"Removed data from BOM table where project == '{project}'")
        self.__exec__()

    def stock_commit(self, project: list[str]) -> None:
        """message method"""
        self.message.append(
            f"Added to stock data from BOM table where project == {project}"
        )
        self.__exec__()

    def stock_use(  # pylint: disable=too-many-positional-arguments,too-many-arguments
        self,
        project: list[str] | None = None,
        dev_id: str = "",
        dev_man: str = "",
        no_devs: bool = False,
        no_stock: bool = False,
        not_enough: bool = False,
    ) -> None:
        """message method"""
        if no_devs:
            self.message.append(f"No device {dev_id} from {dev_man}. Skiped.")
            self.__exec__()
            return
        if no_stock:
            self.message.append("No devices in stock. Stock is empty.")
            self.__exec__()
            return
        if not_enough:
            if project:
                self.message.append(f"Not enough stock for project: {project}.")
            else:
                self.message.append(
                    f"Not enough stock for device {dev_id} from {dev_man}."
                )
            self.__exec__()
            return
        if project:
            self.message.append(f"Removed from stock data for project: {project}.")
        else:
            self.message.append(f"Removed device {dev_id} from {dev_man}.")
        self.__exec__()

    def bom_prepare_projects(
        self,
        project: list[str],
        all_projects: list[str],
    ) -> None:
        """
        if empty projects and all_projects: no projects
        show all projects when user request
        or matched projects when user abreviated
        """
        if not project and not all_projects:
            self.message.append("No projects in BOM table")
        if project != []:
            self.message.append(f"Matched projects:'{project}'")
        if all_projects != []:
            self.message.append(f"Available projects are: {all_projects}")
        self.__exec__()

    def bom_import_summary(self, dat: pd.DataFrame, ex_devs: int = 0) -> None:
        """message method"""
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
                self.message.append(
                    f"{len(dat)} With cost of {cost}{DISP_CURR} in total."
                )
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

    def bom_info(
        self,
        must_cols: list[str],
        nice_cols: list[str],
        col_desc: dict[str, str],
    ) -> None:
        """message method"""
        self.message.append("these columns MUST be present in import file:")
        self.__print_col_description__(cols=must_cols, col_desc=col_desc)
        self.message.append("")
        self.message.append(
            "these columns are optional (but recomended) in import file:"
        )
        self.__print_col_description__(cols=nice_cols, col_desc=col_desc)
        self.__exec__()

    def msg(self, msg: str) -> None:
        """message method"""
        self.message.append(msg)
        self.__exec__()

    def trans_summary(self, txt: list[dict]) -> None:
        """message method"""
        self.message.append("")
        self.message.append("*********************************************************")
        self.message.append("______TRANSACTION_______")
        for d in txt:
            if d["shop"]:
                self.message.append(
                    f"Shopping cart for shop '{d['shop']}' "
                    + f"saved in '{d['file']}_{d['shop']}' in '{d['dir']}'"
                )
                self.message.append(f"Shopping cart value: '{d['price']}'$")
            else:
                self.message.append(
                    f"Shopping cart saved in '{d['file']}' in '{d['dir']}'"
                )
        self.message.append("*********************************************************")
        self.__exec__()

    def unknown_import(self, er: BaseException) -> None:
        """message method"""
        self.message.append(f"Unexpected error: {er}")
        self.message.append("Possibly wrong file format (different shop?) or wrong csv")
        self.__exec__()

    def log_path_error(self, err: str) -> None:
        """message method"""
        self.message.append(err)
        self.message.append(f"Provide correct path in '{CONFIG_PATH}' file.")
        self.__exec__(warning=True)

    def unknown_project(self, project: str, projects: list[str]) -> None:
        """message method"""
        self.message.append(f"Unknown project: '{project}'.")
        self.message.append(f"Available project are: {projects}")
        self.__exec__()

    def project_as_filename(self) -> None:
        """message method"""
        self.message.append("Set project name as file name.")
        self.message.append("You can change later with admin commands.")
        self.__exec__()

    def __exec__(self, warning: bool = False) -> None:
        """message method"""
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
