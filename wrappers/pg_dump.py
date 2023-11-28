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

    def dump(self, task, config, database, backup_path):
        # /usr/bin/pg_dump
        # --verbose
        # --host=10.2.11.43
        # --port=5444
        # --username=dpustula
        # --format=c
        # --encoding=UTF-8
        # --no-owner
        # --file /home/dpustula/Vaults/Cez/DevOps/db-backup/go-20.11.2023/dump-_dictionaries_dev_go-202311201944.backup
        # -n public _dictionaries_dev_go

        credentials = config['credential']

        backup_file_path = os.path.join(backup_path, database["name"] + ".bak")

        schema = []
        if 'schema' in database:
            schema = database['schema']

        checksum_file = None

        if 'generate_checksum' in config['params']:
            checksum_path = os.path.join(backup_path, database["name"] + ".csv")
            checksum_file = generate_checksum(config['host'], config['port'], database["name"], credentials['login'],
                                              credentials['password'], checksum_path, schema)

        cmd = [self.pg_dump_path,
               f'--dbname={database["name"]}',
               f'--username={credentials["login"]}',
               f'--host={config["host"]}',
               f'--port={str(config["port"])}',
               f'--file={backup_file_path}',
               '--format=c',
               '--verbose',
               '--no-owner',
               '--encoding=UTF-8']

        for s in schema:
            cmd.append(f'--schema={s}')

        print(" ".join(cmd))
        print(f'Dumping {database["name"]}')
        p = subprocess.Popen(cmd, env=dict(PGPASSWORD=credentials['password']), text=True,
                             stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        ignore, out = p.communicate()

        print(f'Return code: {p.returncode}')

        if out:
            print(out)

        print(f'{database["name"]} dumped')

        report_path = os.path.join(backup_path, database["name"] + "-dump.report")
        with open(report_path, 'w') as report:
            report.write(f'Report for dumping {database["name"]} from task id {task["id"]}\n')
            report.write(f'Executed command: {" ".join(cmd)}\n')
            report.write(f'Exit code: {p.returncode}\n')
            report.write(f'Logs: {out}\n')

            report.write(f'Generated backup: {backup_file_path}\n')
            report.write(f'Generated checksum: {checksum_file}\n')
            report.flush()
            report.close()

        return backup_file_path, checksum_file, report_path
