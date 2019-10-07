
========================
SnoVault JSON-LD Database Framework
========================

Version 0.1

|Build status|_

.. |Build status| image:: https://travis-ci.org/ENCODE-DCC/snovault.png?branch=master
.. _Build status: https://travis-ci.org/ENCODE-DCC/snovault

Installation Instructions
=========================

Currently these are for Mac OSX.  For linux, look at cloud-config.yml it should be reasonably easy to infer from that

Step 0: Install Xcode (from App Store) and homebrew: http://brew.sh::

Step 1: Verify that homebrew is working properly::

    $ sudo brew doctor


Step 2: Install or update dependencies::

    $ brew install libevent libmagic libxml2 libxslt openssl postgresql graphviz nginx python3
    $ brew install freetype libjpeg libtiff littlecms webp  # Required by Pillow
    $ brew tap homebrew/versions
    $ brew install elasticsearch@5.6 node@10

If you need to update dependencies::

    $ brew update
    $ brew upgrade
    $ rm -rf snowflakes/eggs


Step 3: Run buildout::

    $ python3 bootstrap.py --buildout-version 2.9.5 --setuptools-version 36.6.0
    $ bin/buildout

    NOTE:
    If you have issues with postgres or the python interface to it (psycogpg2) you probably need to install postgresql
    via homebrew (as above)
    If you have issues with Pillow you may need to install new xcode command line tools:
    - First update Xcode from AppStore (reboot)
    $ xcode-select --install
    If you are running macOS Mojave, you may need to run the below command as well:
    $ sudo installer -pkg /Library/Developer/CommandLineTools/Packages/macOS_SDK_headers_for_macOS_10.14.pkg -target /



If you wish to completely rebuild the application, or have updated dependencies:
    $ make clean

    Then goto Step 3.

Step 4: Start the application locally

In one terminal startup the database servers and nginx proxy with::

    $ bin/dev-servers development.ini --app-name app --clear --init --load

This will first clear any existing data in /tmp/snowflakes
Then postgres and elasticsearch servers will be initiated within /tmp/snowflakes.
An nginx proxy running on port 8000 will be started.
The servers are started, and finally the test set will be loaded.

In a second terminal, run the app with::

    $ bin/pserve development.ini

Indexing will then proceed in a background thread similar to the production setup.

Browse to the interface at http://localhost:8000/.


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
