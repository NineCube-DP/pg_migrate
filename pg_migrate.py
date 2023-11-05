import argparse
import os
from datetime import datetime
import yaml

from version import pg_migrate_version
from wrappers.pg_dump import dump_remote, pg_dump_version


def print_version():
    print('pg_migrate version: ' + pg_migrate_version())
    print('pg_dump version: ' + pg_dump_version())


def read_configuration(config_path):
    with open(config_path, "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)


def make_path(config, database_name):
    root_dir = os.path.join(config['task']['path'])
    os.makedirs(root_dir, exist_ok=True)

    backup_dir = os.path.join(root_dir, database_name + '_' + datetime.today().strftime('%Y-%m-%d_%H-%M-%S'))
    os.makedirs(backup_dir, exist_ok=True)

    return root_dir, backup_dir


def initialize(args):
    if args.version:
        print_version()
        exit(0)

    config = read_configuration(args.config_location)

    root_path, db_dir = make_path(config, config['task']['id'])

    if config['task']['mode'] == 'BACKUP':
        backup_config = config['source']
        for database in backup_config['database']:

            try:
                schema = (database['schema'])
            except IndexError:
                schema = None

            dump_remote(
                host=backup_config['host'],
                port=backup_config['port'],
                database=database['name'],
                username=backup_config['credential']['login'],
                password=backup_config['credential']['password'],
                schema=schema,
                path=db_dir,
                checksum='generate_checksum' in backup_config['params']
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
