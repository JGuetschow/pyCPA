#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr  3 22:23:38 2020

@author: Johannes Gütschow, mail@johannes-guetschow.de
"""
# based on scmdata setup.py

import versioneer
from setuptools import setup, find_packages


PACKAGE_NAME = "pyCPA" # has to be changed, already taken
DESCRIPTION = "Climate Policy Analysis using Python"
KEYWORDS = ["climate policy", "greenhouse gas", "emissions", "climate change",
            "climate policy analysis", "climate"]

AUTHORS = [
    ("Johannes Gütschow", "mail@johannes-guetschow.de"),
]
EMAIL = "mail@johannes-guetschow.de"
URL = "https://github.com/JGuetschow/pyCPA"
PROJECT_URLS = {
    "Bug Reports": "https://github.com/JGuetschow/pyCPA/issues",
    #"Documentation": "https://pyCPA.readthedocs.io/en/latest",
    "Source": "https://github.com/JGuetschow/pyCPA",
}
LICENSE = "GNU General Public License v3.0"
CLASSIFIERS = [
    "Development Status :: 2 - Pre-Alpha",
    "License :: OSI Approved :: GNU General Public License v3.0",
    "Intended Audience :: Developers",
    "Operating System :: OS Independent",
#    "Programming Language :: Python :: 3.5", # needs testing 
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
]

REQUIREMENTS = ["scmdata>=0.7.3", "pyam-iamc>=0.7.0"]
#REQUIREMENTS_TESTS = ["codecov", "nbval", "pytest-cov", "pytest>=5.0.0"]
REQUIREMENTS_DOCS = ["sphinx", "sphinx_rtd_theme"]
REQUIREMENTS_DEPLOY = ["setuptools"]

REQUIREMENTS_DEV = [
#    *["flake8", "isort", "nbdime", "notebook", "scipy"],
#    *REQUIREMENTS_TESTS,
    *REQUIREMENTS_DOCS,
    *REQUIREMENTS_DEPLOY,
]

REQUIREMENTS_EXTRAS = {
    "docs": REQUIREMENTS_DOCS,
#    "tests": REQUIREMENTS_TESTS,
    "deploy": REQUIREMENTS_DEPLOY,
    "dev": REQUIREMENTS_DEV,
}


SOURCE_DIR = "src"

PACKAGES = find_packages(SOURCE_DIR)  # no exclude as only searching in `src`
PACKAGE_DIR = {"": SOURCE_DIR}
PACKAGE_DATA = {"pyCPA": ["metadata"]}


README = "README.rst"

with open(README, "r") as readme_file:
    README_TEXT = readme_file.read()

cmdclass = versioneer.get_cmdclass()

setup(
    name=PACKAGE_NAME,
    version=versioneer.get_version(),
    #version="0.0.1",
    description=DESCRIPTION,
    long_description=README_TEXT,
    long_description_content_type="text/markdown",
    author=", ".join([author[0] for author in AUTHORS]),
    author_email=", ".join([author[1] for author in AUTHORS]),
    url=URL,
    project_urls=PROJECT_URLS,
    license=LICENSE,
    classifiers=CLASSIFIERS,
    keywords=KEYWORDS,
    packages=PACKAGES,
    package_dir=PACKAGE_DIR,
    package_data=PACKAGE_DATA,
    include_package_data=True,
    install_requires=REQUIREMENTS,
    extras_require=REQUIREMENTS_EXTRAS,
    python_requires='>=3.6',
    cmdclass = cmdclass,
)
