# INVentory app

Inventory managing app. Created for tracking electronics component in home
projects but probably useful for any other (small) inventory.

Version 0.9.0 , should be fully functional.

Application is interfaced by command line.

Application require vim to be installed. For fuzzy search there are also other dependencies:

- FZF
- wl-copy (WAYLAND)
- notify-send (KDE)
- awk, cut, tr, cat

Data stored in SQLite. SQL scheme described in configuration file so should
be reasonably easy to fine tune.

Data added mainly by importing from excel files. In configuration file can be defined
column name mapping. For now only rules for excel created by LCSC, easyEDA and mouser
are defined.

App developed with following workflow in mind:

1. read BOM file from EDA tool (I'm using easyEDA)

2. for better volume management, possible to import BOM for many projects

3. prepare shop list in shop format to check availability and prices

4. not necessary but advised, import shop cart for prices and availability per shop.

5. export BOM again, after adding cost and availability, to use as import file for
web_shop. This allow split between shops considering best cost.

6. finally all data can be stored in stock table.

## FUZZY SEARCH

Exploring stock (copy or removing single device) can work very well in pipe
with FZF. Bash script is in ./conf folder: inv_fzf.sh.
Most convenient is to plug it to zshrc keybinding:

```bash
#.zshrc
fuzzy_search_inventory()
{
    ./conf/inf_fzf.sh
}
zle -N fuzzy_search_inventory
bindkey '^s' fuzzy_search_inventory
```

For full functionality (clipboard copying and notifications), the script
require WAYLAND and KDE (very platform dependent).

## CONFIGURATION

Application is logging all commands and output into log file.
Global configuration is in `./conf/` directory. You can also have a local config
by putting `.conf/inventory.toml` file in current folder (or sub-folder). You
can use admin --set_local_config option to do so. Local config will use local
sql db, log file and alternative_manufacturers. Of course you can adjust local
configuration manually also.

## NOT IMPLEMENTED / WISH LIST

0. when import stock, --overwrite option fail when used for importing new
data (not existing)

1. when file not present in search, very misleading info about missing folder

2. add summary:

    - cost of devs in stock,
    - projects coverage by stock
    - project cost
