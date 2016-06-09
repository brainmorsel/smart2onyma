import click

from . import export


@click.group()
def main():
    pass


@main.command()
@click.option('--append', default=False, is_flag=True, help='Append new data to existed export')
@click.option('--accs-list-file')
@click.option('--prev-conn-file')  # Файл conn.csv с предыдущей выгрзки, для использования USRCONNID
@click.option('--accs-skip-file')  # список лицевых, которые нужно пропустить
@click.option('--tariffs-history-from')  # дата, с которой выгружать историю тарифов yyyy-mm-dd
@click.option('--data-items', help='accounts, attributes, connections, balances, payments')
@click.argument('profiles', nargs=-1)
def clientdata(append, profiles, accs_list_file, prev_conn_file, accs_skip_file, tariffs_history_from, data_items):
    accs_list = None
    if accs_list_file:
        accs_list = []
        with open(accs_list_file) as f:
            for line in f:
                accs_list.append(line.strip())

    accs_skip = None
    if accs_skip_file:
        with open(accs_skip_file) as f:
            accs_skip = [line.strip() for line in f]

    for profile in profiles:
        bde = export.BillingDataExporter(profile, accs_list,
                accs_skip=accs_skip,
                tariffs_history_from=tariffs_history_from
                )
        if not append:
            bde.clear_output_files()
        if data_items:
            bde.set_export_data_items(data_items.split(','))
        if prev_conn_file:
            bde.load_sitename_to_usrconnid_map(prev_conn_file)
        bde.export_one_by_one()
        append = True


@main.command()
@click.option('--append', default=False, is_flag=True, help='Append new data to existed export')
@click.argument('profiles', nargs=-1)
def tariffs(append, profiles):
    for profile in profiles:
        bde = export.BillingDataExporter(profile)
        if not append:
            bde.clear_output_files()
        bde.export_tariffs()
        append = True


@main.command()
@click.option('--append', default=False, is_flag=True, help='Append new data to existed export')
@click.argument('profiles', nargs=-1)
def tariffs_srv_credit(append, profiles):
    for profile in profiles:
        bde = export.BillingDataExporter(profile)
        if not append:
            bde.clear_output_files()
        bde.export_srv_credit_tariffs()
        append = True


@main.command()
@click.argument('profiles', nargs=-1)
def show_base_companies(profiles):
    for profile in profiles:
        bde = export.BillingDataExporter(profile)
        bde.show_base_companies()


@main.command()
@click.option('--append', default=False, is_flag=True, help='Append new data to existed export')
@click.argument('profiles', nargs=-1)
def policy(append, profiles):
    for profile in profiles:
        bde = export.BillingDataExporter(profile)
        if not append:
            bde.clear_output_files()
        bde.export_policy()
