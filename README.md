# inventory
Inventory managing app. Created for tracking electronics component in home projects but probably usefull for any other (small) inventory.

Version 0.5.0 , for testing

Comand line interface.
Data stored in sqlite. SQL cheme described in configuration file so should be resonably easy to fine tune.
Data added mainly by importing from excel files. In configuration file can be defined column name maping. For now only rules for excel created by LCSC, easyEDA and mouser are defined. 
App developed with followin workflow in mind:
1. read BOM file from EDA tool (i'm usind easyEDA)
2. for better volume management, possible to import BOM for many projects
3. export merged BOM to use as import file for web_shop
4. possible (and advised) to import shop cart.
5. when shop data available, BOM export consider best cost shop, and split shopping lists by shop
NOT IMPLEMENTED
6. BOM can be comited to write a project, and fill stock table based on ordered quantity
