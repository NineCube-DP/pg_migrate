import os
import subprocess

from wrappers.db_tools import verify_checksum


def pg_restore_version():
    cmd = ['bin/pg_restore', '-V']
    result = subprocess.run(cmd, stdout=subprocess.PIPE)
    return result.stdout.decode()


def restore_remote(host, port, database, username, password, path, file, schema=None, checksum=False):
    cmd = ['bin/pg_restore',
           f'--clean',
           f'--create',
           f'--dbname={database}',
           f'--username={username}',
           f'--host={host}',
           f'--port={str(port)}',
           '--format=c',
           '--encoding=UTF-8',
           f'< {file}']
    for s in schema:
        cmd.append(f'--schema={s}')

    print(f'Restoring {database}')
    p = subprocess.Popen(cmd, env=dict(PGPASSWORD=password), text=True, stdout=subprocess.PIPE)
    out, err = p.communicate()

    if out:
        print(f'pg_restore log: {out}')
    if err:
        print(f'pg_restore error: {err}')

    print(f'{database} restored')

    if checksum:
        verify_checksum(host, port, database, username, password, path, schema)

    pass
