import argparse
import os
from datetime import datetime

import yaml

from version import pg_migrate_version
from wrappers import PgDump, PgRestore

pg_dump = PgDump()
pg_restore = PgRestore()


def print_version():
    print('pg_migrate version: ' + pg_migrate_version())
    print('pg_dump version: ' + pg_dump.version())
    print('pg_restore version: ' + pg_restore.version())


def read_configuration(config_path):
    with open(config_path, "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)


def make_path(path):
    backup_dir = os.path.join(path, datetime.today().strftime('%Y-%m-%d_%H-%M-%S'))
    os.makedirs(backup_dir, exist_ok=True)

    return backup_dir


def find_restore_database_conf(databases, name):
    for database in databases:
        if database['source'] == name:
            return database

    return None


def initialize(args):
    if args.version:
        print_version()
        exit(0)

    config = read_configuration(args.config_location)

    task = config['task']

    if (task['mode'] == 'BACKUP' or
            task['mode'] == 'MIGRATE'):
        backup_config = config['source']

        for source_database in backup_config['database']:
            db_dir = make_path(task['path'])

            backup_file, checksum_file, report_file = pg_dump.dump(
                task=task,
                config=backup_config,
                database=source_database,
                backup_path=db_dir
            )

            if task['mode'] == 'MIGRATE':
                restore_config = config['destination']

                restore_database = find_restore_database_conf(restore_config['database'], source_database['name'])

                pg_restore.restore(
                    database=restore_database,
                    backup_file=backup_file,
                    checksum_file=checksum_file,
                    config=restore_config
                )

    if task['mode'] == 'RESTORE':
        restore_config = config['destination']
        for database in restore_config['database']:
            pg_restore.restore(
                database=database,
                backup_file=database['file'],
                checksum_file=database['checksum'],
                config=config['destination']
            )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--version", action='store_true', help="Print tools version")
    parser.add_argument("-c", "--config_location", type=str, default='~/.pg_migrate/tasks',
                        help="Set path to config file, default path ~/.pg_migrate/tasks")

    args = parser.parse_args()

    initialize(args)


if __name__ == '__main__':
    main()
