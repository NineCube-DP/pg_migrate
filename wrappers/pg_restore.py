import subprocess

from wrappers.db_tools import verify_checksum


class PgRestore:

    def __init__(self):
        cmd = ['command -v pg_restore']
        result = subprocess.run(cmd, stdout=subprocess.PIPE, shell=True)
        self.pg_restore_path = str(result.stdout.decode()).strip()

        cmd = ['command -v createdb']
        result = subprocess.run(cmd, stdout=subprocess.PIPE, shell=True)
        self.createdb_path = str(result.stdout.decode()).strip()

    def version(self):
        cmd = [self.pg_restore_path, '-V']
        result = subprocess.run(cmd, stdout=subprocess.PIPE)
        return result.stdout.decode()

    def restore(self, database, backup_file, checksum_file, config):

        cmd = [self.createdb_path,
               f'--host={config["host"]}',
               f'--port={str(config["port"])}',
               f'--username={config["credential"]["login"]}',
               f'--owner={database["owner"] if database["owner"] else config["credential"]["login"]}',
               '--template=template0',
               f'{database["name"]}']
        print(" ".join(cmd))
        print(f'Creating database: {database["name"]}')
        p = subprocess.Popen(cmd, env=dict(PGPASSWORD=config["credential"]["password"]), text=True,
                             stdout=subprocess.PIPE)
        out, err = p.communicate()

        if out:
            print(f'createdb log: {out}')
        if err:
            print(f'createdb error: {err}')

        print(f'Database {database["name"]} created')

        cmd = [self.pg_restore_path,
               f'--dbname={database["name"]}',
               f'--username={config["credential"]["login"]}',
               f'--host={config["host"]}',
               f'--port={str(config["port"])}',
               f'--role={database["owner"] if database["owner"] else config["credential"]["login"]}',
               '--format=c',
               '--no-owner',
               '--clean',
               '--if-exists',
               f'{backup_file}']

        print(" ".join(cmd))
        print(f'Restoring {database["name"]}')
        p = subprocess.Popen(cmd, env=dict(PGPASSWORD=config["credential"]["password"]), text=True,
                             stdout=subprocess.PIPE)
        out, err = p.communicate()

        if out:
            print(f'pg_restore log: {out}')
        if err:
            print(f'pg_restore error: {err}')

        print(f'{database["name"]} restored')

        if 'params' in config and 'verify_checksum' in config['params']:
            verify_checksum(config, database["name"], checksum_file)

        pass
