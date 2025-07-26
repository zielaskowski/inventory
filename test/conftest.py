"""Pytest configuration and fixtures."""

import importlib
import os

import pytest

from app import admin, common, import_dat, message, sql, tabs, transaction
from conf import config as conf
from inv import cli_parser


@pytest.fixture(name="cli")
def cli_fixture():
    """command line parser"""
    return cli_parser()


@pytest.fixture(name="db_setup")
def db_setup_fixture(tmpdir, monkeypatch, cli):
    """
    Initializes a temporary, isolated testing environment.

    This fixture changes the working directory to a temporary one,
    creates a local `config.py` there, and reloads the `config_import`
    module. The updated `load_config` function handles the rest.
    """
    # 1. Change to a temporary directory for test isolation
    monkeypatch.chdir(tmpdir)

    # 2. Use the app's own function to create a local config file
    args = cli.parse_args(["admin", "--set_local_config"])
    admin.admin(args)

    # 3. Force a reload of the config to pick up the new local file.
    # The new load_config() will handle recalculating all paths.
    importlib.reload(conf)

    # 4. Patch any remaining test-specific values
    # monkeypatch.setattr("DB_FILE", "db.sql")
    monkeypatch.setattr(conf, "SCAN_DIR", "")
    monkeypatch.setattr(conf, "DEBUG", "pytest")

    importlib.reload(sql)
    importlib.reload(admin)
    importlib.reload(import_dat)
    importlib.reload(common)
    importlib.reload(tabs)
    importlib.reload(transaction)
    importlib.reload(message)
    # 5. Initialize the database, which will now use the correct temp paths
    sql.sql_check()


@pytest.fixture(name="find_file_fix")
def find_file_fixture(monkeypatch, tmpdir, cli):
    """fixture for import testing"""
    # change to a temporary directory for test isolation
    monkeypatch.setattr(os, "getcwd", lambda: str(tmpdir))
    # create local config
    args = cli.parse_args(["admin", "--set_local_config"])
    admin.admin(args)

    # 2. Reload the config module to re-evaluate all variables
    importlib.reload(conf)
    monkeypatch.setattr(
        conf,
        "import_format",
        {
            "csv": {"file_ext": "csv"},
            "fake1": "",
            "fake2": "",
            "fake3": "",
            "fake4": "",
        },
    )
    monkeypatch.setattr(conf, "SCAN_DIR", "sub")
    importlib.reload(common)
    importlib.reload(message)
