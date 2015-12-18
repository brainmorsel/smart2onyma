smart2onyma
===

Утилита для экспорта данных из SmartASR в Onyma.

Установка
---

Требуется любой дистрибутив линукс, Python версии не ниже 3.3 и virtualenv.

    git clone https://github.com/the-invoice/smart2onyma.git
    virtualenv -p python3 venv
    . venv/bin/activate
    # для PostgreSQL:
    pip install psycopg2
    # или для Oracle SQL (но сначала нужно кое-что ещё установить):
    pip install cx_oracle
    pip install --editable smart2onyma/
    # можно отключить virtualenv
    deactivate

Запуск
---

    venv/bin/smart2onyma КОМАНДА ПУТЬ/К/ПРОФИЛЮ/ЭКСПОРТА
    # Доступные команды:
    venv/bin/smart2onyma --help
