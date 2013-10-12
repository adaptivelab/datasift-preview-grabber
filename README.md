Datasift Preview Grabber
========================

A script to fetch datasift preview stats for a particular hash, over a particular date range.

Quick Start
------------

If you have Python and virtualenv installed, the simplest way to get set up is to
clone or download all the source files, cd into the directory and then run these
commands for the first time:

    $ virtualenv .env
    $ source .env/bin/activate
    $ python setup.py install

That will install everything you need to run the script.

Running the Script
-----------------

    Usage: datasift_preview_grabber <start_date> <end_date> <stream_hash> <datasift-username> <datasift-apikey>

    Where:
        start_date and end_date are in the format yyyy-mm-dd

The script will split the date range you give it into individual days (a limitation
of the Datasift Preview service as it stands).  Each day costs 20DPU currently so
don't go mental with your date range!

For each day, the script creates a preview job with Datasift.  The script waits for
each job to finish, which can take a while.  When all the jobs are finished, the
results of them are simply printed to stdout.

Developing the Script Further
-----------------------------

If you want to enhance the script in some way, install the libraries from the
test_requirements.txt in the virtualenv you created earlier and make sure the
tests all pass before adding something new:

    $ nosetests

If it's useful, send it back to us in the form of a pull request on Github!
