========
Overview
========

.. start-badges

.. list-table::
    :stub-columns: 1

    * - docs
      - |docs|
    * - tests
      - | |travis| |appveyor| |requires|
        | |codecov|
    * - package
      - | |version| |wheel| |supported-versions| |supported-implementations|
        | |commits-since|
.. |docs| image:: https://readthedocs.org/projects/clusterman/badge/?style=flat
    :target: https://clusterman.readthedocs.io/
    :alt: Documentation Status

.. |travis| image:: https://api.travis-ci.com/photoszzt/clusterman.svg?branch=master
    :alt: Travis-CI Build Status
    :target: https://travis-ci.com/github/photoszzt/clusterman

.. |appveyor| image:: https://ci.appveyor.com/api/projects/status/github/photoszzt/clusterman?branch=master&svg=true
    :alt: AppVeyor Build Status
    :target: https://ci.appveyor.com/project/photoszzt/clusterman

.. |requires| image:: https://requires.io/github/photoszzt/clusterman/requirements.svg?branch=master
    :alt: Requirements Status
    :target: https://requires.io/github/photoszzt/clusterman/requirements/?branch=master

.. |codecov| image:: https://codecov.io/gh/photoszzt/clusterman/branch/master/graphs/badge.svg?branch=master
    :alt: Coverage Status
    :target: https://codecov.io/github/photoszzt/clusterman

.. |version| image:: https://img.shields.io/pypi/v/clusterman.svg
    :alt: PyPI Package latest release
    :target: https://pypi.org/project/clusterman

.. |wheel| image:: https://img.shields.io/pypi/wheel/clusterman.svg
    :alt: PyPI Wheel
    :target: https://pypi.org/project/clusterman

.. |supported-versions| image:: https://img.shields.io/pypi/pyversions/clusterman.svg
    :alt: Supported versions
    :target: https://pypi.org/project/clusterman

.. |supported-implementations| image:: https://img.shields.io/pypi/implementation/clusterman.svg
    :alt: Supported implementations
    :target: https://pypi.org/project/clusterman

.. |commits-since| image:: https://img.shields.io/github/commits-since/photoszzt/clusterman/v0.0.0.svg
    :alt: Commits since latest release
    :target: https://github.com/photoszzt/clusterman/compare/v0.0.0...master



.. end-badges

"command to setup VM cluster on cloud"

* Free software: Apache Software License 2.0

Installation
============

::

    pip install clusterman

You can also install the in-development version with::

    pip install https://github.com/photoszzt/clusterman/archive/master.zip


Documentation
=============


https://clusterman.readthedocs.io/


Development
===========

To run all the tests run::

    tox

Note, to combine the coverage data from all the tox environments run:

.. list-table::
    :widths: 10 90
    :stub-columns: 1

    - - Windows
      - ::

            set PYTEST_ADDOPTS=--cov-append
            tox

    - - Other
      - ::

            PYTEST_ADDOPTS=--cov-append tox
