import os
import re
from datetime import datetime
from decimal import Decimal
import ipaddress
from ipaddress import ip_address, ip_network
from math import log2


from .mapper import maps
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
        self.file.write(';'.join(self.fields) + '\n')

    def write(self, **items):
        record = []
        for field in self.fields:
            record.append(str(items.get(field, '')))
        self.file.write(';'.join(record) + '\n')


class AccountAttrsWriter(Writer):
    def write(self, account_id, attr_value, attr_name, sup_attr_name=None):
        attrs = maps['onyma']['attributes']
        attr_id = attrs.get(attr_name, None)
        sup_attr_id = attrs.get(sup_attr_name, '')

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


class BillingDataExporter:
    def __init__(self, profile_file):
        self.profile = mapper.load_profile(profile_file)
        self.db = db.Engine(self.profile['sql-dialect'], self.profile['connection-uri'])
        self.exporter = Exporter(self.profile.get('export-data-dir', 'export_data/'))

        self.db.tpl_env.globals['filters'] = {}
        for filter in self.profile.get('filters', []):
            self.add_filter(**filter)

        self._limit = self.profile.get('limit', 0)

    def add_filter(self, name, **filter_params):
        self.db.tpl_env.globals['filters'][name] = filter_params

    def reset_filters(self):
        self.db.tpl_env.globals['filters'] = {}

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

    def get_onyma_tech_tariff_id(self, name):
        return mapper.maps['onyma']['technological-tariffs'][name]

    def get_onyma_tariff_id(self, origin_id):
        return self.profile['tariffs-map'][int(origin_id)]

    def get_onyma_ats_name(self, origin_name):
        return origin_name  # TODO пока заглушка

    def clear_output_files(self):
        for name in mapper.maps['export-files'].keys():
            with self.exporter.open(name, mode='w') as file:
                file.write_header()

    def export_tariffs(self):
        with self.db.connect() as c, \
                self.exporter.open('tariffs-list') as t, \
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
                if adsl_re.match(r.name):
                    service_id = self.get_onyma_service_id('fee-internet-adsl')
                else:
                    service_id = self.get_onyma_service_id('fee-internet')
                write_tariff(
                    r,
                    self.get_onyma_tpl_tarrif_id('internet'),
                    service_id
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

    # Большущая страшная функция для выгрузки всего, что можно
    def export_one_by_one(self, debug=False):
        with self.db.connect() as c, \
                open('errors.log', 'w', 1) as errlog, \
                self.exporter.open('accounts-list') as f_acc, \
                self.exporter.open_account_attrs() as f_attr, \
                self.exporter.open('connections-names') as f_cn, \
                self.exporter.open('connections-list') as f_cl, \
                self.exporter.open_connection_props() as f_cp, \
                self.exporter.open('balances-list') as f_bl:

            err_groups = set()
            debug_marks = set()

            # считает предпологаемое количество выгружаемых лицевых с учётом фильтров
            def estimate_count(sql_file):
                r = c.execute(sql_file, estimate_count=True).fetchone()
                return r.count

            def norm_phone_number(number_str):
                return re.sub('[^0-9]', '', str(number_str))

            # выгружает данные для одного лицевого
            def export_one(acc_num):
                r = c.execute('account-base-info.sql', account_number=acc_num).fetchone()

                export_balance(r)

                gid = self.get_onyma_gid(r.group_name)
                if gid is None:
                    errlog.write('group {0}\n'.format(r.group_name))
                    err_groups.add(r.group_name)
                    return False

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
                    DOGCODE=r.account_number,
                    DOGDATE=r.create_date.strftime('%d.%m.%Y')
                )

                f_attr.write(r.id, r.notification_email, 'notify-email')
                f_attr.write(r.id, norm_phone_number(r.notification_sms), 'notify-sms')
                f_attr.write(r.id, norm_phone_number(r.notification_fax), 'notify-fax')
                f_attr.write(r.id, r.manager, 'manager')

                if r.acc_type == 'person':
                    export_person_info(acc_num)
                    debug_marks.add('person')
                else:  # company
                    debug_marks.add('company')
                    export_company_info(acc_num)

                export_contacts(acc_num)
                export_addresses(acc_num, r.acc_type)
                export_connections(acc_num, 'lk')
                export_connections(acc_num, 'internet')
                export_connections(acc_num, 'ctv')

                return True

            def export_person_info(acc_num):
                r = c.execute('account-person-info.sql', account_number=acc_num).fetchone()
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
                    errlog.write('export_company_info {0}\n'.format(acc_num))
                    return

                co_name = r.cpt_short + ' ' + r.co_name
                co_name_full = r.cpt_full + ' ' + r.law_name
                f_attr.write(r.id, co_name, 'name')
                f_attr.write(r.id, co_name_full, 'law-name')
                f_attr.write(r.id, r.inn, 'inn')
                f_attr.write(r.id, r.kpp, 'kpp')
                f_attr.write(r.id, r.ogrn, 'ogrn')
                f_attr.write(r.id, r.okonh, 'okonh')
                f_attr.write(r.id, r.okpo, 'okpo')
                f_attr.write(r.id, r.eisup, 'eisup')

            def export_contacts(acc_num):
                for r in c.execute('account-contacts.sql', account_number=acc_num):
                    if r.type_name == 'notify-email':
                        f_attr.write(r.id, r.info, r.type_name)
                    else:
                        f_attr.write(r.id, norm_phone_number(r.info), r.type_name)

            def export_addresses(acc_num, acc_type):
                for r in c.execute('account-addresses.sql', account_number=acc_num, acc_type=acc_type):
                    f_attr.write(r.id, r.zip, 'zip', r.address_type)
                    f_attr.write(r.id, r.state, 'state', r.address_type)
                    f_attr.write(r.id, r.city, 'city', r.address_type)
                    f_attr.write(r.id, r.street, 'street', r.address_type)
                    f_attr.write(r.id, r.num, 'house', r.address_type)
                    f_attr.write(r.id, r.building or r.block, 'block', r.address_type)
                    f_attr.write(r.id, r.flat, 'flat', r.address_type)
                    f_attr.write(r.id, r.entrance, 'entrance', r.address_type)
                    f_attr.write(r.id, r.floor, 'floor', r.address_type)
                    # В ониме нет (пока?) таких атрибутов
                    f_attr.write(r.id, r.postbox, None, r.address_type)

            def export_connections(acc_num, c_type):
                sql_to_exec = 'connections.sql'

                if c_type == 'lk':
                    # Для личного кабинета совсем другой, более простой, запрос
                    sql_to_exec = 'connections-lk.sql'

                res = c.execute(sql_to_exec, c_type=c_type, account_number=acc_num).fetchall()

                for idx in range(len(res)):
                    debug_marks.add('conn_' + c_type)
                    r = res[idx]
                    resource_id = None

                    if c_type == 'lk':
                        tariff_id = self.get_onyma_tech_tariff_id('lk')
                        status_id = self.get_onyma_status_id('active')
                    else:
                        tariff_id = self.get_onyma_tariff_id(r.tariff_id)
                        status_id = self.get_onyma_status_id(r.status)

                    if c_type == 'lk':
                        name_prefix = 'lc'
                        resource_id = self.get_onyma_resource_id('lk-access')
                        export_connections_lk_props(r)
                    elif c_type == 'internet':
                        name_prefix = 'i'
                        if r.conn_type == 'ipoe':
                            resource_id = self.get_onyma_resource_id('internet-connection-net')
                        else:
                            resource_id = self.get_onyma_resource_id('internet-connection')
                        export_connections_internet_props(r)
                    elif c_type == 'phone':
                        name_prefix = 'tel'
                        resource_id = self.get_onyma_resource_id('phone-number')
                        export_connections_phone_props(r)
                    elif c_type == 'ctv':
                        name_prefix = 'tv'
                        resource_id = self.get_onyma_resource_id('ctv-connection')

                    if len(res) > 1 and idx > 0:
                        conn_name = '{0}{1}_{2}'.format(name_prefix, acc_num, idx)
                    else:
                        conn_name = '{0}{1}'.format(name_prefix, acc_num)

                    date_start = datetime.now().strftime('%d.%m.%Y')

                    remark = (r.description or '').replace('\n', ' ').replace('\r', '').replace(';', '.')
                    remark = remark[:250]
                    f_cn.write(
                        ABONID=r.account_id,
                        DOMAINID=self.get_onyma_domain_id(),
                        SITENAME=conn_name,
                        REMARK=remark
                    )

                    f_cl.write(
                        USRCONNID=r.conn_id,
                        ABONID=r.account_id,
                        SITENAME=conn_name,
                        RESID=resource_id,
                        TMID=tariff_id,
                        BEGDATE=date_start,
                        STATUS=status_id,
                        SHARED=0
                    )

            def export_connections_internet_props(r):
                if r.conn_type == 'pppoe':
                    f_cp.write(r.conn_id, 'internet-login', r.login)
                    f_cp.write(r.conn_id, 'internet-password', r.password)
                    f_cp.write(r.conn_id, 'cypher', 'PLAIN')
                    if r.start_ip == r.end_ip:
                        ip = r.start_ip or 0
                        # один IP адрес
                        if ip == 0:
                            debug_marks.add('ip-dynamic')
                            # динамический
                            # магические константы, смысл приблизительно такой:
                            # к подключению привязывается ресурс Динамический IP
                            res_id = self.get_onyma_resource_id('dynamic-ip-addr')
                            f_cp.write(r.conn_id, 0, res_id, 10)
                        else:
                            debug_marks.add('ip-static')
                            # статический
                            # магические константы, смысл приблизительно такой:
                            # к подключению привязывается ресурс Статический IP
                            res_id = self.get_onyma_resource_id('static-ip-addr')
                            f_cp.write(r.conn_id, 0, res_id, 10)

                            ip_addr = ip_address(ip)
                            pool_name = self.get_onyma_static_ip_pool(ip_addr)
                            if not pool_name:
                                errlog.write('no-pool-for {0} {1}\n'.format(r.account_number, ip_addr))
                            f_cp.write(r.conn_id, 'static-ip-pool-name', pool_name)
                            f_cp.write(r.conn_id, 'static-ip-addr', str(ip_addr))
                    else:
                        # подсеть, на PPPoE этого не должно быть
                        print('OH! Shit! subnet on PPPoE for account: ' + r.account_number)
                elif r.conn_type == 'ipoe':
                    debug_marks.add('ip-net')

                    if r.start_ip == r.end_ip:
                        ip_addr = ip_address(r.start_ip)
                        network = str(ip_addr) + '/32'
                        pool_name = self.get_onyma_static_ip_pool(ip_addr)
                    else:
                        network = two_ip_to_net(r.start_ip, r.end_ip)
                        pool_name = self.get_onyma_static_ip_pool(network)

                    if not pool_name:
                        errlog.write('no-pool-for {0} {1}\n'.format(r.account_number, str(network)))

                    f_cp.write(r.conn_id, 'netflow-collector', r.router)
                    f_cp.write(r.conn_id, 'static-net-pool-name', pool_name)
                    f_cp.write(r.conn_id, 'static-net', network)
                else:
                    print('OH! Shit! unexpected connection type for account: ' + r.account_number)

            def export_connections_lk_props(r):
                f_cp.write(r.conn_id, 'lk-login', r.login)
                f_cp.write(r.conn_id, 'lk-password', r.password)
                f_cp.write(r.conn_id, 'cypher', 'MD5MD5')  # два раза MD5

            def export_connections_phone_props(r):
                p = phone_pools.find(r.phone_number)
                series = '{0}/{1}@{2}'.format(p.start_ani, p.size, p.zone_code)
                ats_name = self.get_onyma_ats_name(r.ats_name)

                f_cp.write(r.conn_id, 'phone-ats-name', ats_name)
                f_cp.write(r.conn_id, 'phone-zone-code', p.zone_code)
                f_cp.write(r.conn_id, 'phone-series', series)
                f_cp.write(r.conn_id, 'phone-number', r.phone_number)

            def export_balance(r):
                promised = Decimal(promised_payments.get(r.account_number, 0))
                balance = Decimal(r.child_balance) - promised
                date = r.now.strftime('%d.%m.%Y %H:%M:%S')
                f_bl.write(
                    DATE=date,
                    DOGCODE=r.account_number,
                    BALANCE=balance
                )

            def export_service_credit(acc_num):
                pass

            print('loading phone number pools...')
            phone_pools = PhoneNumberPools()
            for r in c.execute('phone-number-pools.sql'):
                phone_pools.add(r.start_ani, r.end_ani, r.zone_code, r.comments)

            print('counting accounts...')
            cnt_estimate = estimate_count('accounts-list.sql')
            cnt_processed = 0
            cnt_errors = 0

            print('preload promised payments...')
            promised_payments = {}
            for r in c.execute('account-active-promised-paymens.sql'):
                promised_payments[r.account_number] = r.amount

            print('loading accounts...')
            for r in c.execute('accounts-list.sql'):
                try:
                    if export_one(r.account_number):
                        cnt_processed += 1
                    else:
                        errlog.write('account {0}\n'.format(r.account_number))
                        cnt_errors += 1
                except Exception as err:
                    print(err)
                    print('account: {0}'.format(r.account_number))
                    break

                print('estimate/processed/errors: {0}/{1}/{2}'.format(
                    cnt_estimate, cnt_processed, cnt_errors), end='\r')

                if self._limit and cnt_processed >= self._limit:
                    break

        print('\ndone!')
        if len(err_groups):
            print(err_groups)
        if debug:
            print(debug_marks)
