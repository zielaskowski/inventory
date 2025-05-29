# inventory

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

6. finally all data can be stored in project table and stock table.

NOT IMPLEMENTED

BOM can be comited to write a project, and fill stock table based on ordered quantity

WISH LIST:

1. import from csv (i.e. 'LCSC_csv')

2. when preparing shipping list with transaction option, minimum order quantity
should be considered and it's multiplication (if ord_qty is 20, you can't
order 23, must be 40)
