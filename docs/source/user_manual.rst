.. user_manual:

pyCPA User Manual
=================

Python-based tool for Climate Policy Analysis.

This is early development status and currently not intended for wider use but only a few special tasks.

Currently the main use cases are
* conversion of greenhouse gas emissions data between different sector specifications
* consistency check of datasets
* analysis of data coverage in, i.e. which combinations of metadata values (region, gas, sectors, ...) have data.

The main functionalities are split into different modules. Some of the moules
have their individula package, while other are combined into the pyCPa.tools package.

* :ref:`ref.pycpacore`: The core module contains the main functions of pyCPA. For modification of data the main fucntion is `pyCPA.core.combine_rows` which can add and subtract data respecting metadata and units. The remaining fucntions in the module are used to bring data in the format specified by scmdta and pyCPA.

* :ref:`ref.pycpareaddata`: This module contains functions for data reading. It can read individual files or all files within a given folder. Currently only wide format csv files are suppported. More file formats will be added.

* :ref:`ref.pycpaconversion`: This module contains fucntion for conversion of data beweeen metadata hierarchies, e.g. from one sector classification to another.

* :ref:`ref.pycpadataanalysis`: Currently functions for analyzing data coverage and data consistency are included here.

For more details on modules and functions see the API Reference

Examples can be found in the module documentation in the API Reference and in the examples folder in the git repository.

Functionality and some code will be included in the redesign of the PRIMAP emissions-module.
