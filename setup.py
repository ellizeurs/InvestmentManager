from setuptools import setup, find_packages

setup(
    name='InvestmentManager',
    version='0.1',
    description='Manage your stocks investments',
    author='Ellizeu R',
    author_email='ellizeurs@gmail.com',
    packages=find_packages(),
    install_requires=[
        'sqlalchemy>=1.4.0',
        'yfinance>=0.1.63',
        'yahoo_fin>=0.8.9',
        'numpy>=1.0.0',
        'pandas>=1.0.0',
        'tabula-py>=1.4.1',
        'tabulate>=0.8.9',
        'holidays>=0.11.1',
    ],
)
