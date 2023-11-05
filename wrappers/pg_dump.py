import os
import subprocess
import sys
import csv
import psycopg2


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


def gather_statistic(pg_connection, pg_cursor):
    try:
        pg_cursor.execute("""
            SELECT  n.nspname, c.relname table_name,  c.reltuples::bigint AS count_rows
            FROM   pg_class c
            JOIN   pg_namespace n ON n.oid = c.relnamespace
            WHERE
            n.nspname not in('pg_catalog', 'information_schema'); 
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
            data = gather_statistic(conn, cursor)
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


def pg_dump_version():
    cmd = ['bin/pg_dump', '-V']
    result = subprocess.run(cmd, stdout=subprocess.PIPE)
    return result.stdout.decode()


def dump_remote(host, port, database, username, password, path, schema=None, checksum=False):
    if checksum:
        generate_checksum(host, port, database, username, password, path, schema)

    cmd = ['bin/pg_dump',
           f'--dbname={database}',
           f'--username={username}',
           f'--host={host}',
           f'--port={str(port)}',
           f'--file={os.path.join(path, database)}.bak',
           '--format=c',
           '--encoding=UTF-8']
    for s in schema:
        cmd.append(f'--schema={s}')

    print(f'Dumping {database}')
    p = subprocess.Popen(cmd, env=dict(PGPASSWORD=password), text=True, stdout=subprocess.PIPE)
    out, err = p.communicate()

    if out:
        print(f'pg_dump log: {out}')
    if err:
        print(f'pg_dump error: {err}')

    print(f'{database} dumped')
    pass
