Local Installation
==================

Currently these are for macOS using homebrew. If using linux, install dependencies with a different package manager.

Snovault is known to work with Python 3.6.x and will not work with Python 3.7 or greater. If part of the HMS team, it is recommended to use Python 3.4.3, since that's what is running on our servers. A good tool to manage multiple python versions is `pyenv <https://github.com/pyenv/pyenv>_`. It is best practice to create a fresh Python virtualenv using one of these versions before proceeding to the following steps.

Step 0: Obtain AWS keys. These will need to added to your environment variables or through the AWS CLI (installed later in this process).

Step 1: Verify that homebrew is working properly::

    $ brew doctor


Step 2: Install or update dependencies::

    $ brew install libevent libmagic libxml2 libxslt openssl postgresql graphviz
    $ brew install freetype libjpeg libtiff littlecms webp  # Required by Pillow
    $ brew install elasticsearch@5.6

If you need to update dependencies::

    $ brew update
    $ brew upgrade

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

    Then go to Step 3.
