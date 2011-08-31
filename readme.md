## Purpose

Maintain long term data integrity for a directory structure recursively and
automatically: detect new files, deleted files, warn about changed/modified
files, update checksums accordingly

check files in predefined intervals (e.g. 2 weeks)

uses already existing checksum files (md5/sha1) for initialization

detect errorneous hardware early

## Goal

- at the moment, the commandline backend is being developed
- easy to use GUI (Qt probably) in the future

## Requirements

- *cfv*, get it from http://cfv.sourceforge.net/

## Usage

This is work in progress!
This is experimental!
Use at your own risk!

    $ python2.7 dataverifier.py --help

create database file

    $ python2.7 dataverifier.py create

verify files from a database

    $ python2.7 dataverifier.py verify

## License

[GPL](http://www.gnu.org/licenses/gpl.html)
