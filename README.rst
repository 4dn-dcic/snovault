=============
DCIC Snovault
=============

|Build status|_

.. |Build status| image:: https://travis-ci.org/4dn-dcic/snovault.svg?branch=master
.. _Build status: https://travis-ci.org/4dn-dcic/snovault

.. Important::

 DCIC Snovault is a FORK of `snovault <https://pypi.org/project/snovault/>`_
 created at the `ENCODE DCC project at Stanford <https://github.com/ENCODE-DCC>`_.
 Our fork supports other projects of the
 `4D Nucleome Data Coordination and Integration Center (4DN-DCIC)
 <https://github.com/4dn-dcic>`_.
 Although this software is available as open source software,
 its primary function is to support our layered projects,
 and we are not at this time able to offer any active support for other uses.
 In particular, this fork does not purport to supersede
 the original `snovault <https://pypi.org/project/snovault/>`_.
 we just have a different use case that we are actively exploring.

Overview
========

DCIC Snovault is a JSON-LD Database Framework that serves as the backend for the 4DN Data portal and CGAP. Check out our full documentation `here
<https://snovault.readthedocs.io/en/latest/>`_.

.. note::

    This repository contains a core piece of functionality shared amongst several projects
    in the 4DN-DCIC. It is meant to be used internally by the DCIC team
    in support of `Fourfront <https://data.4dnucleome.org>`_\ ,
    the 4DN data portal, and at this point in time it is not expected to be useful
    in a standalone/plug-and-play way to others.

Installation in 4DN components
==============================

DCIC Snovault is pip installable as the ``dcicsnovault`` package with::

    $ pip install dcicsnovault``

However, at the present time, the functionality it provides might only be useful in conjunction
with other 4DN-DCIC components.

NOTE: If you'd like to enable Elasticsearch mapping with type=nested, set the environment variable "MAPPINGS_USE_NESTED"
or set the registry setting "mappings.use_nested".

Installation for Development
============================

Currently these are for Mac OSX using homebrew. If using linux, install dependencies with a different package manager.

Step 0: Install Xcode
---------------------

Install Xcode (from App Store) and homebrew: http://brew.sh

Step 1: Verify Homebrew Itself
------------------------------

Verify that homebrew is working properly::

    $ brew doctor

Step 2: Install Homebrewed Dependencies
---------------------------------------

Install or update dependencies::

    $ brew install libevent libmagic libxml2 libxslt openssl postgresql graphviz python3
    $ brew install freetype libjpeg libtiff littlecms webp  # Required by Pillow
    $ brew cask install adoptopenjdk8
    $ brew install elasticsearch@5.6

NOTES:

* If installation of adtopopenjdk8 fails due to an ambiguity, it should work to do this instead::

    $ brew cask install homebrew/cask-versions/adoptopenjdk8

* If you try to invoke elasticsearch and it is not found,
  you may need to link the brew-installed elasticsearch::

    $ brew link --force elasticsearch@5.6

* If you need to update dependencies::

    $ brew update
    $ rm -rf encoded/eggs

* If you need to upgrade brew-installed packages that don't have pinned versions,
  you can use the following. However, take care because there is no command to directly
  undo this effect::

    $ brew update
    $ brew upgrade
    $ rm -rf encoded/eggs

Step 3: Running Poetry
----------------------

To locally install using versions of Python libraries that have worked before, use this::

    $ poetry install


Updating dependencies
=====================

To update the version dependencies, use::

    $ poetry update

This command also takes space-separated names of specific packages to update. For more information, do::

    $ poetry help update


Managing poetry.lock after update
---------------------------------

There may be situations where you do this with no intent to check in the resulting updates,
but once you have checked that the updates are sound, you may wish to check the resulting
``poetry.lock`` file.

Publishing
==========

Normally, a successful build on a tagged branch (including a branch tagged as a beta)
will cause publication automatically. The process begins by obtaining the version. You might do

    $ head pyproject.toml

to see the first few lines of `pyproject.toml`, which will contain a line like ``version = 100.200.300``, which
is the ``snovault`` version.  You should prepend the letter ``v`` to that version, and create the tag and push
it to the GitHub server:

    $ git tag v100.200.300
    $ git push origin v100.200.300

Please do NOT use some other syntax for ``git push`` that pushes all of your tags. That might pick up tags that
do not belong on the server and can generally cause tag pollution. Push specifically the tag you intend to publish.

Pushing such a tag should trigger publication automatically within a few minutes.

Manual Publication
------------------

There might be rare occasions where you need to do the publication manually, but normally it is not necessary
or desirable. In most cases, it will either fail or will cause the automatic publication step to fail. The main
case where this is known to be needed is where publication has failed on a tagged branch for reasons other than
the fact of that tag being already published (e.g., a network interruption or a premature shutdown of the GitHub
Actions task). An incomplete publication on GitHub Actions cannot be easily retried, so only in that case you may
need to do:

    $ make publish

However, to do this command locally, you would need appropriate credentials on PyPi for such publication to succeed.
As presently configured, these credentials need to be in the environment variables ``PYPI_USER`` and ``PYPI_PASSWORD``.
The script that runs if you manually attempt ``make publish`` checks that you have properly declared credentials
before it attempts to publish. Note that GitHub Actions is already configured with credentials, so you do not
need to worry about them if you just push a tag and let the pre-defined action do the publication.

Running tests
=============

To run specific tests locally::

    $ bin/test -k test_name

To run with a debugger::

    $ bin/test --pdb

Specific tests to run locally for schema changes::

    $ bin/test -k test_load_workbook

Run the Pyramid tests with::

    $ bin/test

