import click


@click.group()
def cmd():
    ...


@click.group()
def info():
    ...


@info.command()
def archives():
    ...


@info.command()
def tables():
    ...


@info.command()
def fields(table):
    ...


@cmd.group()
def etl():
    ...
