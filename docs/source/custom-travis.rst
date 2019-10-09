================
Travis Build
================

We've dedicated an entire document to Travis builds as we've experienced a myriad of different issues with them. What follows is a guide on how to provision a Docker container to run a Travis build. The point of this would be try to reproduce a build problem that appears on Travis but not on local.


Step 1: Docker
^^^^^^^^^^^^^^^

You will need to install Docker. You can get it `here <https://docs.docker.com/docker-for-mac/install/>`_ . 

Once you have Docker, you'll need to get the runner image. You can get it by running the below command.::

    $ docker run --privileged --name travis-debu -it -u travis travisci/ci-amethyst:packer-1512508255-986baf0 /bin/bash -l

The above command should open a session in the container. If not, start the container and connect to it. After, install the ``travis-build`` tool::

    $ cd ~
    $ mkdir .travis
    $ cd ./travis/
    $ git clone https://github.com/travis-ci/travis-build.git
    $ cd travis-build
    $ gem install travis
    $ bundle install
    $ bundle add travis
    $ bundle binstubs travis


Step 2: Get Source
^^^^^^^^^^^^^^^^^^

Once the container is ready to go, you need to install the source code. Clone whichever project you are working on, in this case Snovault and run::

    $ cd snovault
    $ ~/.travis/travis-build/bin/travis compile > ci.sh

Once you have ``ci.sh`` you need to modify the ``travis_cmd git clone`` command to checkout the branch you'd like to test. 


Step 3: Run
^^^^^^^^^^^

Now that you have ``ci.sh`` and have checked out the appropriate branch, you can now run the script::

    $ bash ci.sh 

You can also run it verbosely with::

    $ bash -x ci.sh

Done, this will execute your Travis build.