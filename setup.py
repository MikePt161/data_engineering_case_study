from setuptools import setup, find_packages


# run with python -m build
# install locally with pip install .
# use sales_data entry point as specified

"""
Print help with the following commands

sales-data -h 

sales-data it-data -h


run sales-data

sales-data it-data ./data/dataset_one.csv ./data/dataset_two.csv it_data_test 
"""

setup(
    name="sales_data",
    version="1.0",
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        "pyspark==3.5.0", 'importlib-metadata; python_version=="3.10"'
    ],
    entry_points={
        "console_scripts": [
            "sales-data = sales_data.main:main"
        ]
    },
)
