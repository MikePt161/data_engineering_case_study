from setuptools import setup, find_packages

setup(
    name="data_engineering_app",
    version="0.0.1",
    packages=find_packages(),
    install_requires=[
        "pyspark==3.5.0",
        "chispa==0.10.1",
        'importlib-metadata; python_version=="3.10"'
    ]
)
