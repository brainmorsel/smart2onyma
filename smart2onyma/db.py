from os import path

import jinja2
import sqlalchemy


class Engine:
    'Обёртка над подключением к базе данных, формирует запросы из шаблонов.'
    def __init__(self, sql_dialect, connection_uri, tpl_path=None, debug=False):
        self._debug = debug

        if sql_dialect == 'oracle':
            conn_str = 'oracle+cx_oracle://' + connection_uri
        elif sql_dialect == 'postgres':
            conn_str = 'postgresql+psycopg2://' + connection_uri
        else:
            raise Exception('Unknown SQL dialect.')

        self.sql_dialect = sql_dialect
        self.db = sqlalchemy.create_engine(conn_str, echo=False)

        if tpl_path is None:
            tpl_path = path.join(path.dirname(path.realpath(__file__)), 'sql/')
        self.tpl_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(tpl_path, followlinks=True),
            line_comment_prefix='--'
        )
        self.tpl_env.globals['sql_dialect'] = sql_dialect

    def _execute(self, connection, template, args_dict):
        tpl = self.tpl_env.get_template(template)
        sql = tpl.render(args_dict)

        if self._debug:
            print(sql)

        sql = sqlalchemy.sql.text(sql)
        return connection.execute(sql, **args_dict)

    def connect(self):
        return Connection(self)


class Connection:
    'Обёртка-менеджер контекста для подключения к БД.'
    def __init__(self, engine):
        self.engine = engine

    def __enter__(self):
        self.conn = self.engine.db.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()

    def execute(self, template, **args):
        return self.engine._execute(self.conn, template, args)
