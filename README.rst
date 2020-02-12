========
Snovault
========

|Build status|_

.. |Build status| image:: https://travis-ci.org/4dn-dcic/snovault.svg?branch=master
.. _Build status: https://travis-ci.org/4dn-dcic/snovault

Overview
========

Snovault is a JSON-LD Database Framework that serves as the backend for the 4DN Data portal and CGAP. Check out our full documentation `here
<https://snovault.readthedocs.io/en/latest/>`_.


Installation
============

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

Step 3: Running Buildout
------------------------

Run buildout::

    $ python3 bootstrap.py --buildout-version 2.9.5 --setuptools-version 36.6.0
    $ bin/buildout


NOTES:

* If you have issues with postgres or the python interface to it (psycogpg2)
  you probably need to install postgresql via homebrew (as above)

* If you have issues with Pillow you may need to install new xcode command line tools.

  - First update Xcode from AppStore (reboot)::

      $ xcode-select --install

  - If you are running macOS Mojave (though this is fixed in Catalina), you may need to run this command as well::

      $ sudo installer -pkg /Library/Developer/CommandLineTools/Packages/macOS_SDK_headers_for_macOS_10.14.pkg -target /

  - If you have trouble with zlib, especially in Catalina, it is probably because brew installed it
    in a different location. In that case, you'll want to do the following
    in place of the regular call to buildout::

      $ CFLAGS="-I$(brew --prefix zlib)/include" LDFLAGS="-L$(brew --prefix zlib)/lib" bin/buildout

* If you wish to completely rebuild the application, or have updated dependencies,
  before you go ahead, you'll probably want to do::

    $ make clean

  Then goto Step 3.

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

