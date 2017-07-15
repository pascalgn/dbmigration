# Database migration tool

A tool to export data from one SQL database and import it into another.

## Usage

    mkdir /tmp/db
    vi /tmp/db/migration.properties
    java com.github.pascalgn.dbmigration.Main /tmp/db

## Binary format

The exported files are written in the following format:

    <content> ::= <version> <table-name> <columns> <rows>

    <version> ::= "1"

    <table-name> ::= <text>

    <columns> ::= <column-count> { <column-name> <column-sql-type> }
    <column-count> ::= int
    <column-name> ::= <text>
    <column-sql-type> ::= int

    ; the <rows> section (and therefore the file) ends with a single "0"
    <rows> ::= { "1" <row> } "0"
    ; each row contains exactly <column-count> entries
    <row> ::= { <row-column> }
    <row-column> ::= "0" | "1" ( <text> | <number> | <length> bytes )

    <text> ::= <length> utf8-encoded-bytes
    <length> ::= int
    <number> ::= <scale> <length> bytes
    <scale> ::= int

## License

This database migration tool is licensed under the Apache License, Version 2.0
