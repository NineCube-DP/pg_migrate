import csv
import sys

import psycopg2
import psycopg2.errors
import psycopg2.extras

import pandas as pd


class ExecutionQueryException(Exception):
    def __init__(self, msg):
        self.msg = msg
        super().__init__(self.msg)

    pass


def connect_db(host, port, database, username, password):
    try:
        return psycopg2.connect(database=database,
                                user=username,
                                password=password,
                                host=host,
                                port=port)
    except psycopg2.DatabaseError as e:
        print(f'Database connecting error {e}')
        sys.exit(1)


def run_analyze_database(cursor, schema=None):
    if schema:
        cursor.execute(f"""
            do
            $body$
                declare
                    v_rec record ;
                begin
                    for v_rec in select concat('ANALYZE ', lower(table_schema), '.', table_name, ' ;') query
                                 from information_schema.tables
                                 where table_schema not in ('pg_catalog', 'information_schema')
                                   and table_type = 'BASE TABLE' and table_type='BASE TABLE' 
                                   and table_schema in ('{','.join(schema)}')
                        loop
                            execute v_rec.query;
                        end loop;
                end;
            $body$;
        """)
    else:
        cursor.execute(''' 
        do
        $body$
            declare
                v_rec record ;
            begin
                for v_rec in select concat('ANALYZE ', lower(table_schema), '.', table_name, ' ;') query
                             from information_schema.tables
                             where table_schema not in ('pg_catalog', 'information_schema')
                               and table_type = 'BASE TABLE'
                    loop
                        execute v_rec.query;
                    end loop;
            end;
        $body$; ''')


def gather_statistic(pg_connection, pg_cursor, schema=None):
    try:
        if schema:
            pg_cursor.execute(f"""
                SELECT  n.nspname, c.relname table_name,  c.reltuples::bigint AS count_rows
                FROM   pg_class c
                JOIN   pg_namespace n ON n.oid = c.relnamespace
                WHERE
                n.nspname in ('{','.join(schema)}'); 
            """)
        else:
            pg_cursor.execute("""
                SELECT  n.nspname schema, c.relname table_name,  c.reltuples::bigint AS count_rows
                FROM   pg_class c
                JOIN   pg_namespace n ON n.oid = c.relnamespace
                WHERE
                n.nspname not in ('pg_catalog', 'information_schema'); 
            """)
        pg_connection.commit()
    except psycopg2.errors.SyntaxError as ex:
        print(ex)

    return pg_cursor.fetchall()


def generate_checksum(host, port, database, username, password, path, schema=None):
    conn = connect_db(host, port, database, username, password)
    try:
        with conn.cursor() as cursor:
            run_analyze_database(cursor, schema)
            cursor.close()
            conn.commit()

        with conn.cursor(name=f'pg_migrate_gather_information_cursor', withhold=True) as cursor:
            data = gather_statistic(conn, cursor, schema)
            with open(f'{path}/{database}.csv', 'w') as out:
                csv_out = csv.writer(out)
                csv_out.writerow(['schema', 'table_name', 'table_rows'])
                for row in data:
                    csv_out.writerow(row)
                print(f'Checksum for {database} generated : {path}/{database}.csv')

            cursor.close()
            conn.commit()

            conn.close()
    except ExecutionQueryException as ex:
        pass
    pass


def verify_checksum(config, database, checksum_file, schema=None, ):
    conn = connect_db(config["host"], config["port"], database, config["credential"]["login"],
                      config["credential"]["password"])
    try:
        with conn.cursor() as cursor:
            run_analyze_database(cursor, schema)
            cursor.close()
            conn.commit()

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            data = gather_statistic(conn, cursor)
            cursor.close()
            conn.commit()

        conn.close()
    except ExecutionQueryException as ex:
        pass

    df = pd.DataFrame.from_records(data)

    with open(checksum_file, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=",")
        # ['schema', 'table_name', 'table_rows']
        for row in csv_reader:
            schema = row['schema']
            table_name = row['table_name']
            table_rows = int(row['table_rows'])

            row_count = int(df['count_rows'].loc[(df['schema'] == schema) & (df['table_name'] == table_name)].values[0])

            if table_rows != row_count:
                print(f'Table {table_name} row count mismatch, actual value is {table_rows}, should be {row_count}')
