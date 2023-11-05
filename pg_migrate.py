import argparse
import os
from datetime import datetime

import yaml

from wrappers.pg_dump import dump_remote, pg_dump_version
from setup import VERSION


def print_version():
    print('pg_migrate version: ' + VERSION)
    print('pg_dump version: ' + pg_dump_version())


def read_configuration(config_path):
    with open(config_path, "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)


def initialize(args):
    if args.version:
        print_version()
        exit(0)

    config = read_configuration(args.config_location)

    if config['mode'] == 'BACKUP':
        backup_config = config['source']
        for database in backup_config['database']:
            backup_dir = os.path.join(config['path'])

            try:
                schema = str(database).split('@')[1]
                database_name = str(database).split('@')[0]
            except IndexError:
                schema = None
                database_name = database

            os.makedirs(backup_dir, exist_ok=True)
            dump_remote(
                host=backup_config['host'],
                port=backup_config['port'],
                database=database_name,
                username=backup_config['credential']['login'],
                password=backup_config['credential']['password'],
                schema=schema,
                path=os.path.join(backup_dir,
                                  database_name + '_' + datetime.today().strftime('%Y-%m-%d_%H-%M-%S') + '.bak')
            )


def main():
    # Parse arguments
    parser = argparse.ArgumentParser()
    # parser.add_argument("-t", "--clipboard_TTL", type=int,
    #                     help="Set clipboard TTL (in seconds, default: 15)", nargs='?', const=15)
    # parser.add_argument("-p", "--hide_secret_TTL", type=int,
    #                     help="Set delay before hiding a printed password (in seconds, default: 15)", nargs='?', const=5)
    # parser.add_argument("-a", "--auto_lock_TTL", type=int,
    #                     help="Set auto lock TTL (in seconds, default: 900)", nargs='?', const=900)
    # parser.add_argument("-v", "--vault_location",
    #                     type=str, help="Set vault path")
    # parser.add_argument("-c", "--config_location",
    #                     type=str, help="Set config path")
    # parser.add_argument("-k", "--change_key",
    #                     action='store_true', help="Change master key")
    # parser.add_argument("-i", "--import_items", type=str,
    #                     help="File to import credentials from")
    # parser.add_argument("-x", "--export", type=str,
    #                     help="File to export credentials to")
    # parser.add_argument("-f", "--file_format", type=str, help="Import/export file format (default: 'json')",
    #                     choices=['json'], nargs='?', default='json')
    # parser.add_argument("-e", "--erase_vault", action='store_true',
    #                     help="Erase the vault and config file")

    parser.add_argument("-v", "--version", action='store_true', help="Print tools version")
    parser.add_argument("-c", "--config_location", type=str, default='~/.pg_migrate/tasks',
                        help="Set path to config file, default path ~/.pg_migrate/tasks")

    args = parser.parse_args()

    initialize(args)


if __name__ == '__main__':
    main()
