# Database migration tool

A tool to export data from one SQL database and import it into another.

## Usage

    mkdir /tmp/db
    vim /tmp/db/migration.properties
    java com.github.pascalgn.dbmigration.Main migrate /tmp/db

### Configuration

If no configuration file can be found, a default configuration file will be written.
See [migration-defaults.properties](src/main/resources/com/github/pascalgn/dbmigration/migration-defaults.properties) for more information.

### JDBC drivers

Standard [JDBC](https://en.wikipedia.org/wiki/Java_Database_Connectivity) will be used to access the databases.

The following drivers have been tested to work:

- [H2](http://repo2.maven.org/maven2/com/h2database/h2/1.4.192/)
- [MS SQL](http://repo2.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/6.1.0.jre8/)
- [Oracle](http://www.oracle.com/technetwork/database/features/jdbc/index-091264.html) (registration required)

### SSH tunnel

To connect to a database through an SSH tunnel, use the following command:

    # forwards 127.0.0.1:12345 to sql-server:1433
    ssh -N user@ssh-server -L 127.0.0.1:12345:sql-server:1433

Make sure to also change your migration configuration accordingly:

    ...
    # host and port need to be separated by comma for MS SQL
    source.jdbc.url=jdbc:sqlserver://127.0.0.1,12345;database=dbname
    ...

## Docker image

This tool is also available as a [docker image](https://hub.docker.com/r/pascalgn/dbmigration/):

    $ mkdir /tmp/db
    $ vim /tmp/db/migration.properties
    $ docker run -v /tmp/db:/home/dbmigration/data pascalgn/dbmigration \
          migrate /home/dbmigration/data

You can use the `LOG_LEVEL` environment variable to change the log output:

    $ docker run -e LOG_LEVEL=debug -v /tmp/db:/home/dbmigration/data pascalgn/dbmigration

The default level is `info`. For more information, see the [Dockerfile](src/build/Dockerfile).

## Binary format

The exported files are gzip compressed and written in the following format:

    <content> ::= <version> <table-name> <columns> <rows>

    <version> ::= "3"

    <table-name> ::= <text>

    <columns> ::= <column-count> { <column-name> <column-sql-type> }
    <column-count> ::= int
    <column-name> ::= <text>
    <column-sql-type> ::= int

    <rows> ::= { "1" <row> }
    ; each row contains exactly <column-count> entries
    <row> ::= { <row-column> }
    <row-column> ::= "0" | "1" ( <text> | <number> | <length> bytes | <date> )

    <text> ::= <length> utf8-encoded-bytes
    <length> ::= int
    <number> ::= <scale> <length> bytes
    <scale> ::= int
    ; dates are represented as milliseconds since January 1, 1970
    <date> ::= long

Note that the exported files may consist of multiple [gzip member](https://tools.ietf.org/html/rfc1952#page-5) entries.

## License

This database migration tool is licensed under the Apache License, Version 2.0
