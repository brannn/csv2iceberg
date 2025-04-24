from setuptools import setup, find_packages

setup(
    name="csv_to_iceberg",
    version="0.1.0",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'urllib3==1.26.6',
        'click==8.1.8',
        'email-validator==2.2.0',
        'flask==3.1.0',
        'flask-sqlalchemy==3.1.1',
        'gunicorn==23.0.0',
        'lmdb==1.6.2',
        'polars==1.27.1',
        'psycopg2-binary==2.9.10',
        'pyarrow==19.0.1',
        'pyhive==0.7.0',
        'pyiceberg==0.9.0',
        'rich==13.9.4',
        'thrift==0.21.0',
        'trino==0.333.0',
    ],
    entry_points={
        'console_scripts': [
            'csv-to-iceberg=csv_to_iceberg.main:main',
        ],
    },
)