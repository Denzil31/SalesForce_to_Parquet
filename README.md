## SalesForce to Parquet
#### Script to fetch SalesForce data using the SalesForce API and generate `.parquet` files.
#### Written in python3, implemented using [simple-salesforce](https://github.com/simple-salesforce/simple-salesforce).

### Requirements
> `config.ini` - config file containing credentials. ([Un-populated Example](empty_config.ini))

> `salesforce.json` - list of SF objects, fields and data types. ([Example](salesforce.json))

### Usage
```
$ python main.py -h
usage: main.py [-h] [--exec_type {BULK,NORMAL}] [--log_level {INFO,DEBUG,WARNING,ERROR,CRITICAL}] 
               [--output_path OUTPUT_PATH] [--log_path LOG_PATH]
               config json

Multi-thread enabled Salesforce Data to Parquet/CSV script

positional arguments:
  config                Config file name
  json                  Json file name

options:
  -h, --help            show this help message and exit
  --exec_type {BULK,NORMAL}
                        Select execution type (default NORMAL)
  --log_level {INFO,DEBUG,WARNING,ERROR,CRITICAL}
                        Select Log Level (default INFO)
  --log_path LOG_PATH   Log path (default ./logs)
  --output_path OUTPUT_PATH
                        Output path (defaults to source file path)
```

### Examples
```
$ python main.py config.ini salesforce.json

$ python main.py config.ini salesforce.json --exec_type BULK --output_path out
```

### Output
#### `.parquet` and `.csv` files generated at `output_path` with name of the SF Object

---
### Installation
#### Prerequisites
- Python 3.8 or above
```
/* in the application folder */
$ python -m venv venv
$ source venv/bin/activate
(venv) $ pip install -r requirements.txt
..
(venv) $ deactivate
$
```
