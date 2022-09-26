# Datastore Rust (DSR)
A datastore for the command line.

DSR is a cli frontend for a sqlite database that stores key-value pairs in a table. 

## Usage
```
dsr 0.1.0
Ella Pash
A datastore for the command line

USAGE:
    dsr [OPTIONS] <SUBCOMMAND>

OPTIONS:
        --ds <DS>    Specify datastore location
    -h, --help       Print help information
    -V, --version    Print version information

SUBCOMMANDS:
    contains    Check if a record exists
    delete      Delete a record
    get         Get the value of a record
    help        Print this message or the help of the given subcommand(s)
    keys        Get a list of all keys in the datastore
    records     Get a list of all records in the datastore
    set         Set the value of a record
    values      Get a list of all values in the datastore
```

## License
This software is provided under the MIT license. Click [here](LICENSE) to view.

