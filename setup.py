# Author: Souvik Pratiher
# Project: Valido v0.1.0

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="valido",                     # This is the name of the package
    version="0.1.0",                        # The initial release version
    author="Souvik Pratiher",                     # Full name of the author
    author_email="spratiher9@gmail.com",
    description="PySpark dataframes based workflow validator",
    long_description=long_description,      # Long description read from the the readme file
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),    # List of all python modules to be installed
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
    ],                                      # Information to filter the project on PyPi website
    python_requires='>=3.8',                # Minimum version requirement of the package
    py_modules=["valido"],             # Name of the python package
    install_requires=[
        "atomicwrites",
        "attrs",
        "colorama",
        "iniconfig",
        "numpy",
        "packaging",
        "pandas",
        "pluggy",
        "py",
        "py4j",
        "pyparsing",
        "pyspark",
        "pytest",
        "pytest-mock",
        "python-dateutil",
        "pytz",
        "six",
        "toml"
    ]                     # Install other dependencies if any
)
