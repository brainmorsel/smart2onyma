from setuptools import setup, find_packages

setup(
    name='smart2onyma',
    version='0.1',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'Click',
        'SQLAlchemy',
        'jinja2',
        'PyYAML'
    ],
    entry_points='''
        [console_scripts]
        smart2onyma=smart2onyma.main:main
    ''',
)
