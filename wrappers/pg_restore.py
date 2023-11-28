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

        if False:
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

        # /usr/bin/pg_restore
        # --verbose
        # --host=etransplant-go.ezdrowie.gov.pl
        # --port=2345
        # --username=postgres
        # --role=transplant
        # --clean
        # --no-owner
        # --create
        # --format=c
        # --dbname=transplant_db /home/dpustula/Vaults/Cez/DevOps/db-backup/go-20.11.2023/dump-_etransplant_dev_go-202311201932.backup

        cmd = [self.pg_restore_path,
               f'--dbname={database["name"]}',
               f'--username={config["credential"]["login"]}',
               f'--host={config["host"]}',
               f'--port={str(config["port"])}',
               '--format=c',
               '--no-owner',
               # '--clean',
               # '--create',
               '--exit-on-error',
               '--verbose',
               # '--if-exists',
               f'{backup_file}']

        if database["owner"]:
            cmd.append(f'--role={database["owner"]}')
        else:
            cmd.append(f'--role={config["credential"]["login"]}')

        print(" ".join(cmd))
        print(f'Restoring {database["name"]}')
        p = subprocess.Popen(cmd, env=dict(PGPASSWORD=config["credential"]["password"]), text=True,
                             stderr=subprocess.PIPE)
        out = p.communicate()

        if out:
            print(f'pg_restore log: {out}')

        print(f'{database["name"]} restored')

        if 'params' in config and 'verify_checksum' in config['params']:
            errors = verify_checksum(config, database["name"], checksum_file)

        parent_dir = backup_file.parent.absolute()
        report_path = os.path.join(parent_dir, database["name"] + "-restore.report")
        with open(report_path, 'w') as report:
            report.write(f'Report for restoring {database["name"]} from task id {task["id"]}\n')
            report.write(f'Executed command: {" ".join(cmd)}\n')
            report.write(f'Exit code: {p.returncode}\n')
            report.write(f'Logs: {out}\n')

            report.write(f'Used backup: {backup_file}\n')
            report.write(f'Used checksum: {checksum_file}\n')
            report.write(f'Calculated different:\n')

            if errors:
                report.write('Table,source_rows_count,destination_rows_count')
            for err in errors:
                report.write(f"{err},{err[source_row_count]},{err[restored_row_count]}")

            report.flush()
            report.close()
