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
        credentials = config['credential']

        backup_file_path = os.path.join(backup_path, database["name"] + ".bak")

        schema = None
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
               '--encoding=UTF-8']

        for s in schema:
            cmd.append(f'--schema={s}')

        print(" ".join(cmd))
        print(f'Dumping {database["name"]}')
        p = subprocess.Popen(cmd, env=dict(PGPASSWORD=credentials['password']), text=True,
                             stdout=subprocess.PIPE)
        out, err = p.communicate()

        print(f'Return code: {p.returncode}')

        if out:
            print(f'pg_dump log: {out}')
        if err:
            print(f'pg_dump error: {err}')

        print(f'{database["name"]} dumped')

        report_path = os.path.join(backup_path, database["name"] + ".rep")
        with open(report_path, 'w') as report:
            report.write(f'Report for dumping {database["name"]} from task id {task["id"]}\n')
            report.write(f'Executed command: {" ".join(cmd)}\n')
            report.write(f'Exit code: {p.returncode}\n')
            report.write(f'Info: {out}\n')

            if err:
                report.write(f'Error: {err}\n')

            report.write(f'Generated backup: {backup_file_path}\n')
            report.write(f'Generated checksum: {checksum_file}\n')
            report.flush()
            report.close()

        return backup_file_path, checksum_file, report_path
