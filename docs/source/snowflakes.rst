Snowflakes
================

General
-----------------

Snowflakes used to be the front-end component of Snovault meant to serve as a demo. Since we at 4DN have our own Snovault-backed application (Fourfront, CGAP), snowflakes has been entirely removed from our version of Snovault. It is still present in ENCODE's version which you can find `here <https://github.com/ENCODE-DCC/snovault>`_ .

Removing Snowflakes from Snovault proved more challenging than one may expect. Some parts of snowflakes were actually required for snovault to run, such as ``root.py``. These files have all been migrated into Snovault.

Testing
-----------------

In addition, several relevant tests that lived in Snowflakes have been migrated into Snovault. These tests include only those that are specific to Snovault and are not covered in existing Fourfront/CGAP testing. Properly configuring the tests proved challenging as the test framework as previously configured intertwined Snowflakes and Snovault in such a way that Snovault tests could not function without the presence of Snovault.

To fix this, several aspects of the tests have been refactored. We now load test schemas from files and have migrated many of the relevant fixtures from Snowflakes. ``config.py`` also required changes to account for behavior Snovault expected that it inherited from Snowflakes due to how includes work in PyTest.

Test coverage for Snovault should still be fairly strong, especially when combined with that of Fourfront/CGAP. Some indexing tests are marked as flaky as we've found they experience intermittent failures. Updating how we clear the SQS queue has also helped to remedy this issue.

Troubleshooting Notes
---------------------

One issue of note that was not solved involved a particular logging related test that appears to pass on local and fail on Travis. The associated test is ``test_indexing_logging``. This tests makes a index post on the application and checks to see that a correct log message was emitted. The log message itself is emitted but for some reason on Travis it is truncated. Even spinning up Travis on an identical container could not reproduce the issue. The relevant line is marked in the test file.
