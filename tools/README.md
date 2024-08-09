# Miscellaneous Tools

## [qb_importer.py](qb_importer.py)

Import backups from [QuickBackupM](https://github.com/TISUnion/QuickBackupM) to Prime Backup

```bash
$ python3 qb_importer.py --help
usage: qb_importer.py [-h] -i INPUT -x EXECUTABLE -d DB [-t TEMP] [-c CREATOR] [-s SLOT]

A tool to import backups from QuickBackupMulti to PrimeBackup

Example usages:

  qb_importer.py --help
  qb_importer.py -i ./qb_multi -x ./plugins/PrimeBackup.pyz -d ./pb_files -c Steve
  qb_importer.py -i ./qb_multi -x ./plugins/PrimeBackup.pyz -d ./pb_files --slot 1

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT, --input INPUT
                        Path to the QuickBackupMulti backup file root, e.g. /path/to/qb_multi 
                        (default: None)
  -x EXECUTABLE, --executable EXECUTABLE
                        Path to the PrimeBackup plugin file (default: None)
  -d DB, --db DB        Path to the PrimeBackup file root that contains the database file and so
                        on, e.g. /path/to/pb_files (default: None)
  -t TEMP, --temp TEMP  Path for placing temp files for import (default: ./qb_importer_temp)
  -c CREATOR, --creator CREATOR
                        Creator of the imported backup (default: QuickBackupM)
  -s SLOT, --slot SLOT  Specified the slot number to import. If not provided, import all slots
                        (default: None)
```
