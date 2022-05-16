#!/usr/bin/env python3

import os
import re
import argparse
import subprocess
import configparser
import shutil


def read_in_chunk(stream: str, separator: str) -> str:
    """ Read in chunk https://stackoverflow.com/questions/47927039/reading-a-file-until-a-specific-character-in-python """
    buffer = ''
    while True:  # until EOF
        chunk = stream.readline(4096)  # 4096
        if not chunk:  # EOF?
            yield buffer
            break
        buffer += chunk
        while True:  # until no separator is found
            try:
                part, buffer = buffer.split(separator, 1)
            except ValueError:
                break
            else:
                yield part


def pg_schema_dump(host: str, dbname: str, port: str, user: str, password: str) -> str:
    """ Get schema dump of postgres db """

    with subprocess.Popen(
        ['pg_dump',
         f"--dbname=postgresql://{user}:{password}@{host}:{port}/{dbname}",
         "--schema-only",
         # '-f', dump_file,
         ],
        stdout=subprocess.PIPE
    ) as pg_dump_proc:
        modified_dump = subprocess.Popen(['sed', '/^--/d;/^\\s*$/d;/^SET/d'], text=True, stdin=pg_dump_proc.stdout, stdout=subprocess.PIPE)  # pylint: disable=R1732
        # pg_dump_proc.wait()
        return modified_dump.stdout


def parse_schema(directory: str, object_type: str, schema: str, object_name: str, definition: str, append: bool) -> None:
    """ Writes or appends to schema file """

    dir_path = f"{directory}/schema/{object_type}/{schema}"

    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    file_name = f"{dir_path}/{object_name}.sql"

    if append:
        if not os.path.exists(file_name):
            with open(file_name, 'a', encoding='utf-8') as file:
                file.write(definition)
        else:
            with open(file_name, 'r+', encoding='utf-8') as file:
                current_content = [e+';\n' for e in read_in_chunk(file, ';\n') if e]
                line_found = any(definition in line for line in current_content)
                if not line_found:
                    file.seek(0, os.SEEK_END)
                    file.write(definition)
    else:
        with open(file_name, 'w', encoding='utf-8') as file:
            file.write(definition)


def parse_object(stream: str, object_type: str, append: bool = False) -> None:
    """ Parses tables, views, materialized views, sequences, types, aggregates, alter table, constraints """

    schema_name = re.match(r"^(CREATE TABLE|CREATE AGGREGATE|CREATE VIEW|CREATE MATERIALIZED VIEW|CREATE TYPE|CREATE SEQUENCE|CREATE FOREIGN TABLE|ALTER TABLE \w+|ALTER TABLE) (\w+).(\w+)", stream, re.I).group(2)
    object_name = re.match(r"^(CREATE TABLE|CREATE AGGREGATE|CREATE VIEW|CREATE MATERIALIZED VIEW|CREATE TYPE|CREATE SEQUENCE|CREATE FOREIGN TABLE|ALTER TABLE \w+|ALTER TABLE) (\w+).(\w+)", stream, re.I).group(3)
    parse_schema(args.directory, object_type, schema_name, object_name, stream, append)


def parse_indexes(stream: str, object_type: str, append: bool = False) -> None:
    """ Parses indexes """
    index_name = re.match(r"^CREATE.* INDEX (\w+) ON (\w+).(\w+)", stream, re.I).group(1)
    schema_name = re.match(r"^CREATE.* INDEX (\w+) ON (\w+).(\w+)", stream, re.I).group(2)
    parse_schema(args.directory, object_type, schema_name, index_name, stream, append)


def parse_function(host: str, dbname: str, port: str, user: str, password: str, stream: str, object_type: str, append: bool = False) -> None:
    """ Parses function definition """

    # see https://www.geeksforgeeks.org/postgresql-dollar-quoted-string-constants/
    # because PG functions' bodies can be written as dollar quote and single quotes
    # so we rely on solely on pg_get_functiondef for parsing functions

    schema_name = re.match(r"^CREATE FUNCTION (\w+).(\w+)", stream, re.I).group(1)
    func_name = re.match(r"^CREATE FUNCTION (\w+).(\w+)", stream, re.I).group(2)

    with subprocess.Popen(
        ['psql',
         f"--dbname=postgresql://{user}:{password}@{host}:{port}/{dbname}",
         "-A",
         "--no-align",
         "--no-psqlrc",
         "--tuples-only",
         f"-c SELECT string_agg(pg_get_functiondef(f.oid), E';\n') || ';' AS def FROM (SELECT oid FROM pg_proc \
             WHERE proname = '{func_name}' AND pronamespace = '{schema_name}'::regnamespace) AS f;"],
        stdout=subprocess.PIPE
         ) as func_def_proc:

        func_def = func_def_proc.communicate()[0].decode('utf-8').strip()

        parse_schema(args.directory, object_type, schema_name, func_name, func_def + '\n', append)


def parse_utility(stream: str, utility_type: str, append: bool = True) -> None:
    """ Parses utilitities such as triggers, ownerships, grants, extensions, comments, mappings, schemas, rules, events, servers """

    parse_schema(args.directory, 'utilities', 'all', utility_type, stream, append)


# TODO: in a case a table depends on a user-defined function, we can simply add a dummy function before the create table

if __name__ == "__main__":
    args_parser = argparse.ArgumentParser(
        description=""" Parses schema dump from pg_dump into nicely arranged schema files """,
        epilog=""" Written by Bolaji Wahab """)
    args_parser.add_argument('--directory', required=True, help="Directory to drop the schema files into")
    args_parser.add_argument('--configfile', required=True, help="Database configuration file, see sample")
    args = args_parser.parse_args()

    config = configparser.ConfigParser()
    config.read(args.configfile)

    postgres_host = config.get('postgresql', 'host')
    postgres_port = config.get('postgresql', 'port')
    postgres_db = config.get('postgresql', 'db')
    postgres_user = config.get('postgresql', 'user')
    postgres_password = config.get('postgresql', 'password')

    # clean up previous parse if it exists
    if os.path.exists(f"{args.directory}/schema"):
        shutil.rmtree(f"{args.directory}/schema")

    with pg_schema_dump(postgres_host, postgres_db, postgres_port, postgres_user, postgres_password) as f:

        for segment in read_in_chunk(f, separator=';\n'):
            if segment:
                segment = segment + ';\n'
                if segment.startswith("CREATE TABLE"):
                    parse_object(segment, 'tables')
            if segment.startswith("ALTER TABLE") and "CLUSTER ON" in segment:
                parse_object(segment, 'tables', True)
            if segment.startswith("ALTER TABLE") and "ADD CONSTRAINT" in segment:
                parse_object(segment, 'tables', True)
            if segment.startswith("CREATE INDEX") or segment.startswith("CREATE UNIQUE INDEX"):
                parse_indexes(segment, 'indexes')
            if segment.startswith("CREATE VIEW"):
                parse_object(segment, 'views')
            if segment.startswith("CREATE MATERIALIZED VIEW"):
                parse_object(segment, 'materialized_views')
            if segment.startswith("CREATE FOREIGN TABLE"):
                parse_object(segment, 'foreign_tables')
            if segment.startswith("CREATE AGGREGATE"):
                parse_object(segment, 'aggregates')
            if segment.startswith("CREATE FUNCTION"):
                parse_function(postgres_host, postgres_db, postgres_port, postgres_user, postgres_password, segment, 'functions')
            if segment.startswith("CREATE TYPE"):
                parse_object(segment, 'types')
            if segment.startswith("CREATE SEQUENCE"):
                parse_object(segment, 'sequences')
            if segment.startswith("CREATE TRIGGER") or segment.startswith("CREATE CONSTRAINT TRIGGER"):
                parse_utility(segment, 'triggers')
            if segment.startswith("ALTER TRIGGER"):
                parse_utility(segment, 'triggers')
            if segment.startswith("CREATE RULE"):
                parse_utility(segment, 'rules')
            if segment.startswith("CREATE SCHEMA"):
                parse_utility(segment, 'schemas')
            if "OWNER TO" in segment or "OWNED BY" in segment:
                parse_utility(segment, 'ownerships')
            if "GRANT" in segment or "REVOKE" in segment:
                parse_utility(segment, 'grants')
            if "SET DEFAULT" in segment:
                parse_utility(segment, 'defaults')
            if segment.startswith("CREATE EXTENSION"):
                parse_utility(segment, 'extensions')
            if segment.startswith("CREATE SERVER"):
                parse_utility(segment, 'servers')
            if segment.startswith("COMMENT"):
                parse_utility(segment, 'comments')
            if segment.startswith("CREATE EVENT TRIGGER"):
                parse_utility(segment, 'events')
            if segment.startswith("CREATE USER MAPPING"):
                parse_utility(segment, 'mappings')
            if segment.startswith("CREATE PUBLICATION"):
                parse_utility(segment, 'publications')
            if segment.startswith("ALTER PUBLICATION") and "OWNER TO" not in segment:
                parse_utility(segment, 'publications')
            if segment.startswith("CREATE SUBSCRIPTION"):
                parse_utility(segment, 'subscriptions')
            if segment.startswith("ALTER SUBSCRIPTION") and "OWNER TO" not in segment:
                parse_utility(segment, 'subscriptions')
