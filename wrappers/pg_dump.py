import os
import subprocess

from wrappers.db_tools import generate_checksum


class PgDump:
    def __init__(self):
        cmd = ['command -v pg_dump']
        result = subprocess.run(cmd, stdout=subprocess.PIPE, shell=True)
        self.pg_dump_path = str(result.stdout.decode()).strip()

    def version(self):
        cmd = [self.pg_dump_path, '-V']
        result = subprocess.run(cmd, stdout=subprocess.PIPE)
        return result.stdout.decode()

    def dump(self, host, port, database, username, password, backup_path, schema=None, checksum=False):
        if checksum:
            generate_checksum(host, port, database, username, password, backup_path, schema)

        cmd = [self.pg_dump_path,
               f'--dbname={database}',
               f'--username={username}',
               f'--host={host}',
               f'--port={str(port)}',
               f'--file={os.path.join(backup_path, database)}.bak',
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
