# INVentory app

Inventory managing app. Created for tracking electronics component in home
projects but probably usefull for any other (small) inventory.

Version 0.9.0 , should be fully functional.

Applcation is interfaced by comand line.

Data require vim to be installed. For fuzzy search there are also other dependencies:

- FZF
- wl-copy (WAYLAND)
- notify-send (KDE)
- awk, cut, tr, cat

Data stored in sqlite. SQL cheme described in configuration file so should
be resonably easy to fine tune.

Data added mainly by importing from excel files. In configuration file can be defined
column name maping. For now only rules for excel created by LCSC, easyEDA and mouser
are defined.

App developed with followin workflow in mind:

1. read BOM file from EDA tool (i'm usind easyEDA)

2. for better volume management, possible to import BOM for many projects

3. prepare shop list in shop format to check availability and prices

4. not necessery but advised, import shop cart for prices and availbility per shop.

5. export BOM again, after adding cost and avaialbility, to use as import file for
web_shop. This allow split between shops considering best cost.

6. finally all data can be stored in stock table.

## FUZZY SEARCH

Exporting can work very well in pipe with FZF. Bash script is in conf folder:
inv_fzf.sh. Most convinient is to plug it to zshrc keybinding:

```bash
#.zshrc
fuzzy_search_inventory()
{
    ./conf/inf_fzf.sh
}
zle -N fuzzy_search_inventory
bindkey '^s' fuzzy_search_inventory
```

For full functionality (clipboard copying and notifcations), the script
require WAYLAND and KDE (very platform dependent).

## NOT IMPLEMENTED / WISH LIST

1. when file not present in search, very misleading info about missing folder

2. add summary:

    - cost of devs in stock,
    - projects coverage by stock
    - project cost

3. add audit_log table and undo for stock manipulation (only stock!):

```sql
CREATE TABLE audit_log (
    id INTEGER PRIMARY KEY,
    table_name TEXT,
    row_id INTEGER,
    operation TEXT,
    old_data TEXT,
    new_data TEXT,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER log_update
AFTER UPDATE ON my_table
BEGIN
  INSERT INTO audit_log(table_name, row_id, operation, old_data, new_data)
  VALUES (
    'my_table', OLD.id, 'UPDATE',
    json_object('col1', OLD.col1, 'col2', OLD.col2),
    json_object('col1', NEW.col1, 'col2', NEW.col2)
  );
END;
```
