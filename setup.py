import re
import shlex
import subprocess
import sys

from setuptools import setup, find_packages
from setuptools.command.install import install

from version import pg_migrate_version


def get_tag_version():
    cmd = 'git tag --points-at HEAD'
    versions = subprocess.check_output(shlex.split(cmd)).splitlines()
    if not versions:
        return None
    if len(versions) != 1:
        sys.exit(f"Trying to get tag via git: Expected exactly one tag, got {len(versions)}")
    version = versions[0].decode()
    if re.match('^v[0-9]', version):
        version = version[1:]
    return version


class VerifyVersionCommand(install):
    """ Custom command to verify that the git tag matches our version """
    description = 'verify that the git tag matches our version'

    def run(self):
        tag_version = get_tag_version()
        if tag_version and tag_version != pg_migrate_version():
            sys.exit(f"Git tag: {tag_version} does not match the version of this app: {pg_migrate_version()}")


setup(
    name='pg_migrate',
    version=pg_migrate_version(),
    packages=find_packages("."),
    license='MIT',
    author='Daniel Pustuła',
    author_email='dpustula@ninecube.pl',
    install_requires=[
    ],
    platforms='any',
    cmdclass={
        'verify': VerifyVersionCommand,
    },
    python_requires=">=3.6",
)
