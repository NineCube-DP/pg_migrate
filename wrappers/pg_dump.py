import os
import subprocess

from wrappers.db_tools import generate_checksum


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
