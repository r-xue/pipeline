.. CASA Pipeline documentation master file, created by
   sphinx-quickstart on Tue Oct  8 14:45:28 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

CASA Pipeline
=============

   The Data Processing Pipeline for standard ALMA and VLA Operations

The ALMA Science Pipeline is used for the automated calibration and
imaging of ALMA interferometric and single-dish data. ALMA
Interferometric data refers to observations obtained with either the
ALMA 12-m Array or 7-m Array, while single-dish data refers to
observations obtained with the 12-m dishes of the ALMA Total Power
Array.

The VLA calibration pipeline performs basic flagging and calibration
using CASA. It is currently designed to work for Stokes I continuum data
(except P-band and 4-band), but can work in other circumstances as well.
Each Scheduling Block (SB) observed by the VLA is automatically
processed through the pipeline.

A VLA imaging pipeline is available for VLA continuum data using the
aggregate bandwidth available in an observation. This imaging pipeline
is built on the foundation provided by the ALMA imaging pipeline, but is
optimized to support the VLA. The current imaging pipeline parameters
may not be optimal for all datasets, but will be applicable for all the
bands supported by the Science Ready Data Products processing.

.. image:: https://user-images.githubusercontent.com/176921/203636195-6762748b-dadf-46c7-a356-16757afbdaec.png


Installation
------------

OS X & Linux:

.. code:: sh

   # download the latest CASA pre-release tarball from https://casa.nrao.edu/download/distro/casa/releaseprep/?C=M;O=D

   wget https://casa.nrao.edu/download/distro/casa/releaseprep/casa-6.5.3-19-py3.8.tar.xz
   tar xf casa-6.5.3-19-py3.8.tar.xz

   # clone the pipeline repository
   git clone https://open-bitbucket.nrao.edu/scm/pipe/pipeline.git pipeline.git
   cd pipeline.git/

   # install additional pipeline dependencies
   casa-6.5.3-19-py3.8/bin/pip3 install astropy bdsf

   # setup and install the pipeline
   casa-6.5.3-19-py3.8/bin/python3 setup.py install

Usage example
-------------

*For more examples and usage, please refer to
the*\ `Wiki <https://github.com/yourname/yourproject/wiki>`__\ *.*

Development setup
-----------------

Describe how to install all development dependencies and how to run an
automated test-suite of some kind. Potentially do this for multiple
platforms.

Release History
---------------

-  0.2.1

   -  CHANGE: Update docs (module code remains unchanged)

-  0.2.0

   -  CHANGE: Remove ``setDefaultXYZ()``
   -  ADD: Add ``init()``

-  0.1.1

   -  FIX: Crash when calling ``baz()`` (Thanks
      @GenerousContributorName!)

-  0.1.0

   -  The first proper release
   -  CHANGE: Rename ``foo()`` to ``bar()``

-  0.0.1

   -  Work in progress

Meta
----

Contact Info – helpdesk@example.edu

Distributed under the XYZ license. See ``LICENSE`` for more information.

`https://github.com/yourname/github-link <https://github.com/>`__

Contributing
------------

1. Fork it (https://github.com/yourname/yourproject/fork)
2. Create your feature branch (``git checkout -b feature/fooBar``)
3. Commit your changes (``git commit -am 'Add some fooBar'``)
4. Push to the branch (``git push origin feature/fooBar``)
5. Create a new Pull Request

.. toctree::
   :hidden:
   :maxdepth: 1

   tasks
   userguide
   publications
   knownissues
   
