from setuptools import find_packages, setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='postgresql-interface',
    packages=find_packages(include=['postgressqlinterface']),
    version='0.0.2',
    description='Library to load and extract data from a PosgreSQL Database '
                'with Python with a simple SQL style language',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Antonio Benjumea',
    license='MIT',
    install_requires=[
        'numpy==1.18.1',
        'pandas==0.25.3',
        'psycopg2-binary==2.8.5'
    ],
    setup_requires=['pytest-runner'],
    tests_requires=[
        'pytest==6.1.2',
        'python-dotenv==0.15.0'
    ],
    test_suite='tests',
    url='https://github.com/antjes88/PostgreSQLInterface'
)
