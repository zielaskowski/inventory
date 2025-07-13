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
So column extraction, copying to clipboard and notification is separated ino
bash script. Clipboard coping and notification is very platform dependent, here
WAYLAND and KDE.

## NOT IMPLEMENTED / PROBLEMS

1. remove commited column and related functions
2. change stock --commit with --use: remove all devs from project from stock
3. remove single dev from stock (with fuzzy search)
4. when file not present in search, very misleading info about missing folder
5. by default devices are not updated. add option to call align_other_cols()
on upcoming data
6. warn when adding existing devs to stock: similar like BOM, add --overwrite option

## WISH LIST

1. when preparing shipping list with transaction option, minimum order quantity
should be considered and it's multiplication (if ord_qty is 20, you can't
order 23, must be 40)
