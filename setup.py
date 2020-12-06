from setuptools import find_packages, setup

setup(
    name='PostgreSQLInterface',
    packages=find_packages(include=['postgressqlinterface']),
    version='0.0.1',
    description='Library to load and extract data from a PosgreSQL Database '
                'with Python with a simple SQL style language',
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
    test_suite='tests'
)
