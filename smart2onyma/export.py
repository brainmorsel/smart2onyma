import os
import re
from datetime import datetime
from datetime import timezone
from datetime import timedelta
import calendar
from decimal import Decimal
import ipaddress
from ipaddress import ip_address, ip_network
from math import log2
from collections import defaultdict


from .mapper import maps, load_sitename_to_usrconnid_map
from . import db
from . import mapper


class Writer:
    def __init__(self, filename, format, mode):
        self.filename = filename
        self.fields = format.split(';')
        self.mode = mode

    def __enter__(self):
        self.file = open(self.filename, self.mode)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.file.close()

    def write_header(self):
        self.file.write(';'.join(self.fields) + ';\n')

    def write(self, **items):
        record = []
        for field in self.fields:
            value = str(items.get(field, ''))
            if ';' in value:
                value = '"{0}"'.format(value)
            record.append(value)
        self.file.write(';'.join(record) + ';\n')


class AccountAttrsWriter(Writer):
    def write(self, account_id, attr_value, attr_name, sup_attr_name=None):
        attrs = maps['onyma']['attributes']
        attr_id = attrs.get(attr_name, None)
        sup_attr_id = attrs.get(sup_attr_name, '')
        if isinstance(attr_value, list):
            attr_value = ', '.join(attr_value)

        if attr_id and attr_value and len(str(attr_value).strip()) > 0:
            attr_value = str(attr_value).strip()
            super().write(
                ABONID=account_id,
                ATTRID=attr_id,
                ATTRIDUP=sup_attr_id,
                VALUE=attr_value
            )


class ConnectionPropsWriter(Writer):
    def write(self, conn_id, property, value, valuenum=''):
        props = maps['onyma']['properties']
        # вернёт либо id, либо само переданое значение
        prop_id = props.get(property, property)
        super().write(
            USRCONNID=conn_id,
            PROPERTY=prop_id,
            VALUE=value,
            VALUENUM=valuenum
        )


class Exporter:
    def __init__(self, data_dir='export_data/'):
        self.data_dir = data_dir
        if not os.path.isdir(data_dir):
            os.makedirs(data_dir)

    def open(self, export_file, mode='a'):
        filename, format = maps['export-files'].get(export_file)
        filename = os.path.join(self.data_dir, filename)
        return Writer(filename, format, mode)

    def open_account_attrs(self):
        filename, format = maps['export-files'].get('accounts-attrs')
        filename = os.path.join(self.data_dir, filename)
        return AccountAttrsWriter(filename, format, 'a')

    def open_connection_props(self):
        filename, format = maps['export-files'].get('connections-props')
        filename = os.path.join(self.data_dir, filename)
        return ConnectionPropsWriter(filename, format, 'a')


def two_ip_to_net(start_ip, end_ip):
    mask = 32 - int(log2(end_ip - start_ip + 1))
    nw = str(ip_address(start_ip)) + '/' + str(mask)
    return ip_network(nw)


class PhoneNumberPool:
    def __init__(self, start_ani, end_ani, zone_code, comments):
        self.start_ani = int(start_ani)
        self.end_ani = int(end_ani)
        self.zone_code = zone_code
        self.comments = comments
        self.size = int(end_ani) - int(start_ani) + 1

    def has(self, number):
        number = int(number)

        return self.start_ani <= number <= self.end_ani


class PhoneNumberPools:
    def __init__(self):
        self.pools = []

    def add(self, start_ani, end_ani, zone_code, comments):
        pool = PhoneNumberPool(start_ani, end_ani, zone_code, comments)
        self.pools.append(pool)

    def find(self, number):
        for pool in self.pools:
            if pool.has(number):
                return pool
        return None


class ErrorsCounter:
    def __init__(self, logfile):
        self.logfile = logfile
        self.count = 0
        self.accounts = set()
        self.groups = set()

    def error(self, account_number, message):
        self.accounts.add(account_number)
        self.logfile.write('{0}: {1}\n'.format(account_number, message))

    def no_group(self, account_number, group_name):
        self.groups.add(group_name)
        self.error(account_number, 'no group map for "{0}"'.format(group_name))


class BillingDataExporter:
    def __init__(self, profile_file, accs_list=None, accs_skip=None, tariffs_history_from=None):
        self.profile = mapper.load_profile(profile_file)
        self.db = db.Engine(self.profile['sql-dialect'], self.profile['connection-uri'])
        self.exporter = Exporter(self.profile.get('export-data-dir', 'export_data/'))

        self.db.tpl_env.globals['filters'] = {}
        for filter in self.profile.get('filters', []):
            self.add_filter(**filter)

        self._limit = self.profile.get('limit', 0)
        self._accs_list = accs_list
        self._accs_skip = set(accs_skip or [])
        self._tariffs_history_from = tariffs_history_from
        self._dayly_write_off_fix = self.profile.get('dayly-write-off-fix', False)

        self._conn_id_next = 1
        self.sitename_to_usrconnid_map = {}

        # какие данные выгружать
        self.export_items = set(['accounts', 'attributes', 'connections', 'balances', 'payments'])

    def set_export_data_items(self, items):
        self.export_items = set(items)

    def load_sitename_to_usrconnid_map(self, filename):
        self.sitename_to_usrconnid_map = load_sitename_to_usrconnid_map(filename)
        self._conn_id_next = max(self.sitename_to_usrconnid_map.values()) + 1

    def add_filter(self, name, **filter_params):
        self.db.tpl_env.globals['filters'][name] = filter_params

    def reset_filters(self):
        self.db.tpl_env.globals['filters'] = {}

    def gen_conn_id(self, conn_name):
        try:
            return self.sitename_to_usrconnid_map[conn_name]
        except KeyError:
            conn_id = self._conn_id_next
            self._conn_id_next += 1
            return conn_id

    def get_onyma_gid(self, name):
        return self.profile['groups-map'].get(name, None)

    def get_onyma_tpl_tarrif_id(self, name):
        return self.profile['tariff-templates'][name]

    def get_onyma_domain_id(self):
        return self.profile['domain-id']

    def get_onyma_static_ip_pool(self, ip):
        for name, pool_addr in self.profile['static-ip-pools'].items():
            pool = ip_network(pool_addr)
            if isinstance(ip, ipaddress.IPv4Network) and ip.overlaps(pool):
                return name
            elif ip in pool:
                return name
        return None

    def get_onyma_utid(self, name):
        return mapper.maps['onyma']['account-types'][name]

    def get_onyma_tsid(self, name):
        return mapper.maps['onyma']['tax-schemas'][name]

    def get_onyma_csid(self, name):
        return mapper.maps['onyma']['credit-schemas'][name]

    def get_onyma_service_id(self, name):
        return mapper.maps['onyma']['service'][name]

    def get_onyma_resource_id(self, name):
        return mapper.maps['onyma']['resources'][name]

    def get_onyma_status_id(self, name):
        return mapper.maps['onyma']['status'][name]

    def get_onyma_property_id(self, name):
        return mapper.maps['onyma']['properties'][name]

    def get_onyma_tech_tariff_id(self):
        return mapper.maps['onyma']['technological-tariff']

    def get_onyma_tariff_id(self, origin_id):
        return self.profile['tariffs-map'].get(int(origin_id), None)

    def get_onyma_ats_name(self, origin_name):
        return origin_name  # TODO пока заглушка

    def clear_output_files(self):
        for name in mapper.maps['export-files'].keys():
            with self.exporter.open(name, mode='w') as file:
                pass
                # file.write_header()

    def export_tariffs(self):
        with self.db.connect() as c, \
                self.exporter.open('tariffs-list') as t, \
                self.exporter.open('tariffs-policy') as t_pol, \
                self.exporter.open('tariffs-prices') as tp:

            def write_tariff(r, onyma_tpl_id, service_id):
                commentary = r.cnt
                end_date = ''
                price = Decimal(r.fee or '0.00')
                price = (price * Decimal(1.18)).quantize(Decimal('1.00'))
                # 2 - архивный
                if r.status == 2:
                    end_date = r.modify_date.strftime('%d.%m.%Y')

                t.write(
                    START_DATE=r.create_date.strftime('%d.%m.%Y'),
                    OLD_TMID=r.id,
                    TMID=onyma_tpl_id,
                    NAME=r.name,
                    COMMENTARY=commentary,
                    PERIOD=r.period or '',
                    NEXT_TMID=r.next_tariff_id or '',
                    END_DATE=end_date
                )
                tp.write(
                    START_DATE=r.create_date.strftime('%d.%m.%Y'),
                    OLD_TMID=r.id,
                    NAME=r.name,
                    TMID='',
                    SERVID=service_id,
                    PRICE=price
                )

            print('loading internet tariffs...')
            # регулярное выражение для определения ADSL тарифов
            adsl_re = re.compile(self.profile.get('tariffs-adsl-match-re', '.*ADSL.*'))
            for r in c.execute('tariffs.sql'):
                if r.forcompany:
                    onyma_tpl_id = self.get_onyma_tpl_tarrif_id('internet-company')
                elif r.forperson:
                    onyma_tpl_id = self.get_onyma_tpl_tarrif_id('internet-person')
                else:
                    onyma_tpl_id = self.get_onyma_tpl_tarrif_id('internet-person')

                if adsl_re.match(r.name):
                    service_id = self.get_onyma_service_id('fee-internet-adsl')
                else:
                    service_id = self.get_onyma_service_id('fee-internet')

                write_tariff(r, onyma_tpl_id, service_id)

                if 'tariffs-policy-map' in self.profile:
                    for p in c.execute('tariff-policy.sql', tariff_id=r.id):
                        policy_name = p.value.replace('ssg-account-info=A', '')
                        policy_id = 0
                        try:
                            policy_id = self.profile['tariffs-policy-map'][policy_name]
                        except KeyError:
                            print('WARNING: no mapping for policy: {0}'.format(policy_name))

                        t_pol.write(
                            START_DATE=r.create_date.strftime('%d.%m.%Y'),
                            OLD_TMID=r.id,
                            NAME=r.name,
                            TMID='',
                            POLID=policy_id
                        )

            print('loading phone tariffs...')
            for r in c.execute('tariffs.sql', phone_tariffs=True):
                write_tariff(
                    r,
                    self.get_onyma_tpl_tarrif_id('phone'),
                    self.get_onyma_service_id('fee-phone-number')
                )

            print('loading ctv tariffs...')
            for r in c.execute('tariffs.sql', ctv_tariffs=True):
                write_tariff(
                    r,
                    self.get_onyma_tpl_tarrif_id('ctv'),
                    self.get_onyma_service_id('fee-ctv')
                )
            print('loading npl tariffs...')
            for r in c.execute('tariffs.sql', npl_tariffs=True):
                write_tariff(
                    r,
                    self.get_onyma_tpl_tarrif_id('npl'),
                    self.get_onyma_service_id('fee-channel')
                )
            print('done!')

    def export_srv_credit_tariffs(self):
        with self.db.connect() as c, \
                self.exporter.open('tariffs-list') as t:
            start_date = datetime.now().strftime('%d.%m.%Y')
            for r in c.execute('service-with-credit-list.sql'):
                onyma_tpl_id = self.get_onyma_tpl_tarrif_id('service-credit')
                t.write(
                    START_DATE=start_date,
                    OLD_TMID=r.id,
                    TMID=onyma_tpl_id,
                    NAME=r.svc_name,
                )
            print('done!')

    def export_policy(self):
        with self.db.connect() as c, \
                self.exporter.open('connections-list') as f_cl, \
                self.exporter.open_connection_props() as f_cp:
            policy_list = {}
            for r in c.execute('policy-items.sql'):
                if r.id not in policy_list:
                    policy_list[r.id] = {
                        'name': r.name,
                        'id': r.id,
                        'items': defaultdict(list)}
                policy_list[r.id]['items'][r.attribute].append(r.value)

            resource_id = self.get_onyma_resource_id('internet-policy')
            date_start = datetime.now().strftime('%d.%m.%Y')
            for conn_id, p in policy_list.items():
                f_cl.write(
                    USRCONNID=conn_id,
                    ABONID=self.profile['base-account-id'],
                    SITENAME=self.profile['base-account-sitename'],
                    RESID=resource_id,
                    TMID=self.get_onyma_tech_tariff_id(),
                    BEGDATE=date_start,
                    STATUS=self.get_onyma_status_id('active'),
                    SHARED=0
                )
                f_cp.write(conn_id, 'policy-CoA-type', 'HIGH')
                f_cp.write(conn_id, 'policy-name', p['name'])
                for key, values in p['items'].items():
                    for idx, val in enumerate(values, start=1):
                        f_cp.write(conn_id, key, val, idx)

    # Большущая страшная функция для выгрузки всего, что можно
    def export_one_by_one(self, debug=False):
        with self.db.connect() as c, \
                open('errors.log', 'w', 1) as errlog, \
                self.exporter.open('accounts-list') as f_acc, \
                self.exporter.open_account_attrs() as f_attr, \
                self.exporter.open('connections-names') as f_cn, \
                self.exporter.open('connections-list') as f_cl, \
                self.exporter.open('connections-status-history') as f_chist, \
                self.exporter.open_connection_props() as f_cp, \
                self.exporter.open('tariffs-personal') as f_tariffs_personal, \
                self.exporter.open('tariffs-history') as f_tariffs_history, \
                self.exporter.open('promised-payments') as f_promised_payments, \
                self.exporter.open('balances-list') as f_bl, \
                self.exporter.open('payments-list') as f_pay:

            _errors = ErrorsCounter(errlog)

            # считает предпологаемое количество выгружаемых лицевых с учётом фильтров
            def estimate_count(sql_file):
                r = c.execute(sql_file, estimate_count=True).fetchone()
                return r.count

            def norm_phone_number(number_str):
                return re.sub('[^0-9]', '', str(number_str))

            # выгружает данные для одного лицевого
            def export_one(acc_num):
                r = c.execute('account-base-info.sql', account_number=acc_num).fetchone()
                if not r:
                    _errors.error(acc_num, 'no such account')
                    return

                if 'accounts' in self.export_items:
                    gid = self.get_onyma_gid(r.group_name)
                    if gid is None:
                        _errors.no_group(acc_num, r.group_name)

                    if r.acc_type == 'person':
                        tsid = self.get_onyma_tsid('person')
                        csid = self.get_onyma_csid('prepay')
                    else:  # company
                        tsid = self.get_onyma_tsid('std')
                        csid = self.get_onyma_csid('unlimited')

                    f_acc.write(
                        ABONID=r.id,
                        GID=gid,
                        TSID=tsid,
                        CSID=csid,
                        DOGCODE=acc_num,
                        DOGDATE=r.create_date.strftime('%d.%m.%Y'),
                        UTID=self.get_onyma_utid(r.acc_type)
                    )

                if 'attributes' in self.export_items:
                    f_attr.write(r.id, r.notification_email, 'notify-email')
                    f_attr.write(r.id, norm_phone_number(r.notification_sms), 'notify-sms')
                    f_attr.write(r.id, norm_phone_number(r.notification_fax), 'notify-fax')
                    f_attr.write(r.id, r.manager, 'manager')

                    if r.acc_type == 'person':
                        export_person_info(acc_num)
                    else:  # company
                        export_company_info(acc_num)

                    export_contacts(acc_num)
                    export_addresses(acc_num, r.acc_type)

                balance_correction = 0
                if 'connections' in self.export_items:
                    balance_correction += export_connections(acc_num, 'lk')
                    balance_correction += export_connections(acc_num, 'internet')
                    balance_correction += export_connections(acc_num, 'ctv')
                    balance_correction += export_connections(acc_num, 'npl')

                if 'balances' in self.export_items:
                    export_balance(r, balance_correction)
                if 'payments' in self.export_items:
                    export_payments(acc_num)

                return True

            def export_person_info(acc_num):
                r = c.execute('account-person-info.sql', account_number=acc_num).fetchone()
                if not r:
                    _errors.error(acc_num, 'export_person_info')
                    return
                if r.birth_day:
                    birth_day = r.birth_day.strftime('%d.%m.%Y')
                    f_attr.write(r.id, birth_day, 'birth-day')
                f_attr.write(r.id, r.birth_place, 'birth-place')
                f_attr.write(r.id, r.secret_word, 'secret')
                f_attr.write(r.id, r.first_name, 'first-name', 'fullname')
                f_attr.write(r.id, r.last_name, 'last-name', 'fullname')
                f_attr.write(r.id, r.second_name, 'second-name', 'fullname')
                f_attr.write(r.id, ' '.join(
                    (r.last_name, r.first_name, r.second_name)), 'name')

                passport_date = ''
                if r.passport_date:
                    passport_date = r.passport_date.strftime('%d.%m.%Y')
                passport = ' '.join((
                    r.passport_series or '',
                    r.passport_number or '',
                    passport_date,
                    r.passport_issuer or ''
                ))
                f_attr.write(r.id, passport, 'passport')

            def export_company_info(acc_num):
                r = c.execute('account-company-info.sql', account_number=acc_num).fetchone()
                if not r:
                    _errors.error(acc_num, 'export_company_info')
                    return

                #co_name = r.cpt_short + ' ' + r.co_name
                #co_name_full = r.cpt_full + ' ' + r.law_name
                co_name = r.co_name
                co_name_full = r.law_name
                f_attr.write(r.id, co_name, 'name')
                f_attr.write(r.id, co_name_full, 'law-name')
                f_attr.write(r.id, r.inn, 'inn')
                f_attr.write(r.id, r.kpp, 'kpp')
                f_attr.write(r.id, r.ogrn, 'ogrn')
                f_attr.write(r.id, r.okonh, 'okonh')
                f_attr.write(r.id, r.okpo, 'okpo')
                f_attr.write(r.id, r.eisup, 'eisup')

            def export_contacts(acc_num):
                contacts = defaultdict(list)
                account_id = None
                for r in c.execute('account-contacts.sql', account_number=acc_num):
                    account_id = r. id
                    if r.type_name == 'extra-email':
                        contacts[r.type_name].append(r.info)
                    else:
                        contacts[r.type_name].append(norm_phone_number(r.info))
                for attr_name, attr_values in contacts.items():
                    f_attr.write(r.id, attr_values, attr_name)

            def export_addresses(acc_num, acc_type):
                for r in c.execute('account-addresses.sql', account_number=acc_num, acc_type=acc_type):
                    f_attr.write(r.id, r.zip, 'zip', r.address_type)
                    f_attr.write(r.id, r.state, 'state', r.address_type)
                    f_attr.write(r.id, r.city, 'city', r.address_type)
                    f_attr.write(r.id, r.street, 'street', r.address_type)
                    f_attr.write(r.id, r.num, 'house', r.address_type)
                    f_attr.write(r.id, r.building or r.block, 'block', r.address_type)
                    f_attr.write(r.id, r.flat, 'flat', r.address_type)
                    #f_attr.write(r.id, r.entrance, 'entrance', r.address_type)
                    #f_attr.write(r.id, r.floor, 'floor', r.address_type)
                    # В ониме нет (пока?) таких атрибутов
                    #f_attr.write(r.id, r.postbox, None, r.address_type)

            def export_connections(acc_num, c_type):
                balance_correction = 0
                now = datetime.now()
                days_in_month = calendar.monthrange(now.year, now.month)[1]
                sql_to_exec = 'connections.sql'

                if c_type == 'lk':
                    # Для личного кабинета совсем другой, более простой, запрос
                    sql_to_exec = 'connections-lk.sql'

                res = c.execute(sql_to_exec, c_type=c_type, account_number=acc_num).fetchall()

                for idx in range(len(res)):
                    r = res[idx]
                    resource_id = None
                    remark = (r.description or '').replace('\n', ' ').replace('\r', '').replace(';', '.')
                    remark = remark[:250]

                    if c_type == 'lk':
                        tariff_id = self.get_onyma_tech_tariff_id()
                        status_id = self.get_onyma_status_id('active')
                    else:
                        tariff_id = self.get_onyma_tariff_id(r.tariff_id)
                        status_id = self.get_onyma_status_id(r.status)

                    if tariff_id is None:
                        _errors.error(acc_num, 'no tariff map for {0}'.format(r.tariff_id))
                        continue

                    if c_type == 'lk':
                        name_prefix = 'lc'
                    elif c_type == 'internet':
                        name_prefix = 'i'
                    elif c_type == 'phone':
                        name_prefix = 'tel'
                    elif c_type == 'ctv':
                        name_prefix = 'tv'
                    elif c_type == 'npl':
                        name_prefix = 'npl'

                    if len(res) > 1 and idx > 0:
                        conn_name = '{0}{1}_{2}'.format(name_prefix, acc_num, idx)
                    else:
                        conn_name = '{0}{1}'.format(name_prefix, acc_num)

                    usrconnid = self.gen_conn_id(conn_name)

                    login = ''
                    if c_type == 'lk':
                        resource_id = self.get_onyma_resource_id('lk-access')
                        export_connections_lk_props(r, usrconnid)
                    elif c_type == 'internet':
                        if self._dayly_write_off_fix and r.status == 'active':
                            balance_correction = - r.tariff_fee / days_in_month
                        if r.conn_type == 'ipoe':
                            resource_id = self.get_onyma_resource_id('internet-connection-net')
                        else:
                            login = r.login
                            resource_id = self.get_onyma_resource_id('internet-connection')
                        export_connections_internet_props(r, usrconnid)
                        export_internet_services(r, conn_name, tariff_id, usrconnid)
                    elif c_type == 'phone':
                        resource_id = self.get_onyma_resource_id('phone-number')
                        export_connections_phone_props(r, usrconnid)
                    elif c_type == 'ctv':
                        resource_id = self.get_onyma_resource_id('ctv-connection')
                    elif c_type == 'npl':
                        resource_id = self.get_onyma_resource_id('internet-npl')
                        comm_parts = []
                        if r.platform1:
                            comm_parts.append(r.platform1)
                        if r.platform2:
                            comm_parts.append(r.platform2)
                        if comm_parts:
                            # f_cp.write(conn_id, 'internet-npl-commentary', ' - '.join(comm_parts))
                            remark = ' - '.join(comm_parts)
                    export_connections_status_history(r.conn_id, usrconnid, login)

                    date_start = datetime.now().strftime('%d.%m.%Y')

                    f_cn.write(
                        ABONID=r.account_id,
                        DOMAINID=self.get_onyma_domain_id(),
                        SITENAME=conn_name,
                        REMARK=remark
                    )

                    f_cl.write(
                        USRCONNID=usrconnid,
                        ABONID=r.account_id,
                        SITENAME=conn_name,
                        RESID=resource_id,
                        TMID=tariff_id,
                        BEGDATE=date_start,
                        STATUS=status_id,
                        SHARED=0,
                        REMARK=r.conn_id  # оригинальный ID объекта авторизации в биллинге
                    )

                    export_service_credit(r, conn_name, tariff_id, usrconnid)

                    if self._tariffs_history_from and c_type != 'lk':
                        for rr in c.execute('tariffs-history.sql', conn_id=r.conn_id, date_from=self._tariffs_history_from):
                            date_now = datetime.now().strftime('%d.%m.%Y %H:%M')
                            date_start = rr.start_date.strftime('%d.%m.%Y %H:%M')
                            tariff_id = self.get_onyma_tariff_id(rr.tariff_id)
                            if tariff_id is None:
                                _errors.error(acc_num, 'no tariff map for {0}'.format(rr.tariff_id))
                                continue
                            f_tariffs_history.write(
                                TMID=tariff_id,
                                DATE_START=date_start,
                                USRCONNID=usrconnid,
                                DATE=date_now,
                                SITENAME=conn_name
                            )

                    if self.profile['discounts-service-mapping']:
                        for rr in c.execute('discounts.sql', conn_id=r.conn_id):
                            onyma_srvid = self.profile['discounts-service-mapping'].get(rr.discount_id)
                            if onyma_srvid is None:
                                _errors.error(acc_num, 'no discount map for {0}'.format(rr.discount_id))
                                continue
                            f_tariffs_personal.write(
                                TMID=tariff_id,
                                ABONID=r.account_id,
                                SITENAME=conn_name,
                                SERVID=onyma_srvid,
                                COST=0,
                                COEF=1.0,
                                CCNTR=1.0,
                                MDATE=rr.start_date.strftime('%d.%m.%Y %H:%M:%S'),
                                USRCONNID=usrconnid,
                                REMARK=rr.description
                            )

                return balance_correction

            def export_connections_status_history(conn_id, usrconnid, login=''):
                # пришлось сюда засунуть сохранение статусов объекта авторизации
                tz = timezone(timedelta(hours=9))  # TODO: make profile option
                first_day = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0, tzinfo=tz)
                last_status = None  # до начала тукещего месяца

                def write_status(date, status):
                    if status is None:
                        return
                    status_date = date.astimezone(tz).strftime('%d.%m.%Y %H:%M:%S')
                    f_chist.write(
                        LOGIN=login,
                        MDATE=status_date,
                        STATUS=self.get_onyma_status_id(status),
                        USRCONNID=usrconnid
                    )

                for h in c.execute('connection-statuses.sql', conn_id=conn_id):
                    # Тут мы отсекаем все статусы до начала текущего месяца
                    # попутно сохраняя последний из них
                    if h.start_date < first_day:
                        last_status = h.status
                        continue
                    elif last_status is not None:
                        write_status(first_day, last_status)
                        last_status = None
                    write_status(h.start_date, h.status)
                # вывести статус на начало месяца, если бельше статусов нет?
                if last_status is not None:
                    write_status(first_day, last_status)


            def export_connections_internet_props(r, conn_id):
                if r.conn_type == 'pppoe':
                    f_cp.write(conn_id, 'internet-login', r.login)
                    f_cp.write(conn_id, 'internet-password', r.password)
                    f_cp.write(conn_id, 'cypher', 'PLAIN')

                    if r.start_ip == r.end_ip:
                        ip = r.start_ip or 0
                        # один IP адрес
                        if ip == 0:
                            # динамический
                            # магические константы, смысл приблизительно такой:
                            # к подключению привязывается ресурс Динамический IP
                            res_id = self.get_onyma_resource_id('dynamic-ip-addr')
                            f_cp.write(conn_id, 0, res_id, 10)
                        else:
                            # статический
                            # магические константы, смысл приблизительно такой:
                            # к подключению привязывается ресурс Статический IP
                            res_id = self.get_onyma_resource_id('static-ip-addr')
                            f_cp.write(conn_id, 0, res_id, 10)

                            ip_addr = ip_address(ip)
                            pool_name = self.get_onyma_static_ip_pool(ip_addr)
                            if not pool_name:
                                _errors.error(r.account_number, 'no ip pool for {0}'.format(ip_addr))
                            f_cp.write(conn_id, 'static-ip-pool-name', pool_name)
                            f_cp.write(conn_id, 'static-ip-addr', str(ip_addr))
                    else:
                        # подсеть, на PPPoE этого не должно быть
                        print('OH! Shit! subnet on PPPoE for account: ' + r.account_number)
                elif r.conn_type == 'ipoe':

                    if r.start_ip == r.end_ip:
                        ip_addr = ip_address(r.start_ip)
                        network = str(ip_addr) + '/32'
                        pool_name = self.get_onyma_static_ip_pool(ip_addr)
                    else:
                        network = two_ip_to_net(r.start_ip, r.end_ip)
                        pool_name = self.get_onyma_static_ip_pool(network)

                    if not pool_name:
                        _errors.error(r.account_number, 'no ip pool for {0}'.format(str(network)))

                    f_cp.write(conn_id, 'netflow-collector', r.router)
                    f_cp.write(conn_id, 'static-net-pool-name', pool_name)
                    f_cp.write(conn_id, 'static-net', network)
                else:
                    print('OH! Shit! unexpected connection type for account: ' + r.account_number)

            def export_connections_lk_props(r, conn_id):
                f_cp.write(conn_id, 'lk-login', r.login)
                f_cp.write(conn_id, 'lk-password', r.password)
                f_cp.write(conn_id, 'cypher', 'MD5MD5')  # два раза MD5

            def export_connections_phone_props(r, conn_id):
                p = phone_pools.find(r.phone_number)
                series = '{0}/{1}@{2}'.format(p.start_ani, p.size, p.zone_code)
                ats_name = self.get_onyma_ats_name(r.ats_name)

                f_cp.write(conn_id, 'phone-ats-name', ats_name)
                f_cp.write(conn_id, 'phone-zone-code', p.zone_code)
                f_cp.write(conn_id, 'phone-series', series)
                f_cp.write(conn_id, 'phone-number', r.phone_number)

            def export_internet_services(r, sitename, tmid, usrconnid):
                "Экспорт периодических услуг на объектах авторизации интернета"
                if r.conn_id not in internet_periodic_services:
                    return

                for srv in internet_periodic_services[r.conn_id]:
                    try:
                        onyma_srvid = self.profile['periodic-service-mapping'][srv.id]
                    except KeyError:
                        _errors.error(r.account_number, 'no mapping for service id {0} ("{1}")'.format(srv.id, srv.name))
                        continue
                    cost = srv.price or srv.count_price
                    cost = (cost * Decimal(1.18)).quantize(Decimal('1.00'))
                    f_tariffs_personal.write(
                        TMID=tmid,
                        ABONID=r.account_id,
                        SITENAME=sitename,
                        SERVID=onyma_srvid,
                        COST=cost,
                        COEF=srv.amount,
                        CCNTR=1.0,
                        MDATE=srv.status_date.strftime('%d.%m.%Y %H:%M:%S'),
                        USRCONNID=usrconnid,
                        SERV_ALIAS=srv.name
                    )

            def export_service_credit(r, sitename, tmid, usrconnid):
                if r.conn_id in credit_services:
                    for item in credit_services[r.conn_id]:
                        try:
                            onyma_srvid = self.profile['credit-service-mapping'][item.type_id]
                        except KeyError:
                            _errors.error(r.account_number, 'no mapping for credit service id {0} ("{1}")'.format(item.type_id, item.name))
                            continue
                        cost = item.credit_monthly_payment
                        #cost = (cost * Decimal(1.18)).quantize(Decimal('1.00'))
                        start_date = item.start_date.strftime('%d.%m.%Y %H:%M:%S')
                        end_date = item.end_date.strftime('%d.%m.%Y %H:%M:%S')

                        f_tariffs_personal.write(
                            TMID=tmid,
                            ABONID=r.account_id,
                            SITENAME=sitename,
                            SERVID=onyma_srvid,
                            COST=cost,
                            COEF=1.0,
                            CCNTR=1.0,
                            MDATE=start_date,
                            USRCONNID=usrconnid,
                            SERV_ALIAS=item.name,
                            ENDDATE=end_date
                        )

            def export_balance(r, balance_correction):
                promised = Decimal('0.00')
                for pp in promised_payments[r.account_number]:
                    promised += Decimal(pp.amount)
                    date_now = datetime.now().strftime('%d.%m.%Y %H:%M:%S')
                    f_promised_payments.write(
                        ABONID=r.id,
                        DOGCODE=r.account_number,
                        CREDIT_SUM=Decimal(pp.amount),
                        ENDDATE=pp.expire_date.strftime('%d.%m.%Y %H:%M:%S'),
                        DATE=date_now
                    )
                balance = Decimal(r.child_balance) - promised + Decimal(balance_correction)
                date = r.now.strftime('%d.%m.%Y %H:%M:%S')
                f_bl.write(
                    DATE=date,
                    DOGCODE=r.account_number,
                    BALANCE=balance
                )

            def export_payments(acc_num):
                for r in c.execute('account-payments.sql', account_number=acc_num):
                    date = r.payment_date.strftime('%d.%m.%Y %H:%M:%S')
                    f_pay.write(
                        DOGCODE=r.account_number,
                        MDATE=date,
                        SUM=r.sum
                    )

            print('loading phone number pools...')
            phone_pools = PhoneNumberPools()
            for r in c.execute('phone-number-pools.sql'):
                phone_pools.add(r.start_ani, r.end_ani, r.zone_code, r.comments)

            if self._accs_list:
                cnt_estimate = len(self._accs_list)
            else:
                print('counting accounts...')
                cnt_estimate = estimate_count('accounts-list.sql')

            print('preload promised payments...')
            promised_payments = defaultdict(list)
            for r in c.execute('account-active-promised-paymens.sql'):
                promised_payments[r.account_number].append(r)

            if self._accs_list:
                accounts = self._accs_list
            else:
                accounts = []
                print('loading accounts...')
                for r in c.execute('accounts-list.sql'):
                    accounts.append(r.account_number)

            print('preload periodic services...')
            internet_periodic_services = {}
            for r in c.execute('service-for-internet.sql'):
                try:
                    internet_periodic_services[r.conn_id].append(r)
                except KeyError:
                    internet_periodic_services[r.conn_id] = [r, ]

            print('preload credit services...')
            credit_services = {}
            for r in c.execute('service-with-credit.sql'):
                try:
                    credit_services[r.conn_id].append(r)
                except KeyError:
                    credit_services[r.conn_id] = [r, ]

            cnt_processed = 0
            for account_number in accounts:
                if account_number in self._accs_skip:
                    continue
                try:
                    export_one(account_number)
                    cnt_processed += 1
                except Exception:
                    import traceback
                    print('')
                    traceback.print_exc()
                    # traceback.print_tb(err.__traceback__)
                    print('account: {0}'.format(account_number))
                    break

                print('estimate/processed/errors: {0}/{1}/{2}                   '.format(
                    cnt_estimate, cnt_processed, len(_errors.accounts)), end='\r')

                if self._limit and cnt_processed >= self._limit:
                    break

        print('\ndone!')

    def show_base_companies(self):
        format = '{0:<10} {2:<10} {1}'
        print(format.format('Company ID', 'Company Name', 'Accounts'))
        print(format.format('----------', '------------', '--------'))
        with self.db.connect() as c:
            for r in c.execute('base-companies.sql'):
                print(format.format(r.base_company_id, r.name, r.cnt))
