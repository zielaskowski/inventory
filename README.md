# INVentory app

Inventory managing app. Created for tracking electronics component in home
projects but probably usefull for any other (small) inventory.

Version 0.5.0 , for testing

Applcation is interfaced by comand line.

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

6. finally all data can be stored stock table.

## FUZZY SEARCH

Exporting can work very well in pipe with FZF:

```bash
#inventory_device_copy.sh
dev_id=$(echo "$1" | awk '{print $8}' | tr -d '\n')
echo -n "$dev_id" | wl-copy 			# copy to clipboard, wayland only
notify-send "Copied to clipboard:" "$dev_id"	# notify what copied KDE only

#.zshrc
fuzzy_search_inventory()
{
    inv bom -e % | \
        fzf --bind "enter:execute-silent(\
                echo {} | \
                xargs -I{} inventory_device_copy.sh {})" \
            --header 'press ENTER to copy to clipboard'
}
zle -N fuzzy_search_inventory
bindkey '^s' fuzzy_search_inventory
```

FZF and zsh do some magic with scripts and it was not possible to define all in `.zshrc`
So column extraction, copying to clipboard and notification is separated into
bash script. Clipboard coping and notification is very platform dependent, here
WAYLAND and KDE.

## NOT IMPLEMENTED / PROBLEMS

1. remove single dev from stock (with fuzzy search)

2. when file not present in search, very misleading info about missing folder

3. by default devices are not updated. add option to call align_other_cols()
on upcoming data

4. summary: cost of devs in stock, projects coverage by stock

5. add selection to alternative_manufacturers also when there is a multiple selection

6. add audit_log table:

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
