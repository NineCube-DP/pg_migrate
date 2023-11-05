import subprocess


def generate_checksum():
    # get information about schema structure, count of tables, sequences, index
    # get information about count of row in tables
    pass


def pg_dump_version():
    cmd = ['bin/pg_dump', '-V']
    result = subprocess.run(cmd, stdout=subprocess.PIPE)
    return result.stdout.decode()


def dump_remote(host, port, database, username, password, path, schema=None, checksum=False):
    if checksum:
        generate_checksum()

    cmd = ['bin/pg_dump',
           f'--dbname={database}',
           f'--username={username}',
           f'--host={host}',
           f'--port={str(port)}',
           f'--file={path}',
           '--format=c',
           '--encoding=UTF-8']
    if schema:
        cmd.append(f'--schema={str(schema).split(",")[0]}')

    print(cmd)
    p = subprocess.Popen(cmd, env=dict(PGPASSWORD=password), text=True, stdout=subprocess.PIPE)
    out, err = p.communicate()
    print(out, err)
    pass
