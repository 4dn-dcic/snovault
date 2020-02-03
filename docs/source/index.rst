Snovault Documentation
========================

|Build status|_

.. |Build status| image:: https://travis-ci.org/4dn-dcic/snovault.svg?branch=master
.. _Build status: https://travis-ci.org/4dn-dcic/snovault

Snovault is a JSON-LD Database Framework that serves as the backend for the `4DN Data portal <https://github.com/4dn-dcic/fourfront>`_ and `CGAP <https://github.com/dbmi-bgm/cgap-portal>`_. It is a very divergent fork of the work of the same name written by the ENCODE team at Stanford University. `See here <https://github.com/ENCODE-DCC/snovault>`_ for the original version.

Since Snovault is used for multiple deployments across a couple projects, we use `GitHub releases <https://github.com/4dn-dcic/snovault/releases>_` to version it. This page also acts as a changelog.

To get started, read the following documentation on setting up and developing Snovault:

    $ bin/test


.. toctree::
   :maxdepth: 2
   :hidden:
   
   index
   overview
   attachment
   auth
   custom-travis
   database
   embedding-and-indexing
   es-mapping
   invalidation
   object-lifecycle
   search_info
   snowflakes
   testing
