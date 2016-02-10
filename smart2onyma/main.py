import click

from . import export


@click.group()
def main():
    pass


@main.command()
@click.option('--append', default=False, help='Append new data to existed export')
@click.option('--accs-list-file')
@click.argument('profiles', nargs=-1)
def clientdata(append, profiles, accs_list_file):
    accs_list = None
    if accs_list_file:
        accs_list = []
        with open(accs_list_file) as f:
            for line in f:
                accs_list.append(line.strip())

    for profile in profiles:
        bde = export.BillingDataExporter(profile, accs_list)
        if not append:
            bde.clear_output_files()
        bde.export_one_by_one()
        append = True


@main.command()
@click.option('--append', default=False, help='Append new data to existed export')
@click.argument('profiles', nargs=-1)
def tariffs(append, profiles):
    for profile in profiles:
        bde = export.BillingDataExporter(profile)
        if not append:
            bde.clear_output_files()
        bde.export_tariffs()
        append = True


@main.command()
@click.option('--append', default=False, help='Append new data to existed export')
@click.argument('profiles', nargs=-1)
def tariffs_srv_credit(append, profiles):
    for profile in profiles:
        bde = export.BillingDataExporter(profile)
        if not append:
            bde.clear_output_files()
        bde.export_srv_credit_tariffs()
        append = True
