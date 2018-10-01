
.. |build_status| image:: https://csce-dirbs-ci-1.qualcomm.com/buildStatus/icon?job=dirbs-build-master
                  :target: https://csce-dirbs-ci-1.qualcomm.com/job/dirbs-build-master/

The Device Identification, Registration, Blocking, System (DIRBS) consists of a Core component, classification engine, and list generation capability along with a collection of subsystem components providing functions that include reporting, registration of IMEIs, verification of IMEI status, pairing of IMEIs with IMSIs to avoid blocking, and reporting of lost & stolen IMEIs. Repositories are provided for each of these components; please see each respective repository for further details on installation and usage. To support Identity & Access Management (IAM) and API gateway functionality, open-source options such as Keycloak (www.keycloak.org) and Tyk (https://tyk.io) are available and may be integrated for a DIRBS deployment.

DIRBS |build_status|
====================

Directory structure
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This repository contains code forming the "core" part of DIRBS. It contains:

  * ``src/dirbs/`` -- The DIRBS core Python package, to be installed on target
    machines. SQL files are included in here as they are distributed as package data.
  * ``docker/`` -- Dockerfiles and shell scripts related to Docker containers.
  * ``etc/`` -- Config files, crontabs, etc. to be deployed to ``/opt/dirbs/etc/``.
  * ``scripts/`` -- Utility scripts that are useful for team members but which
    will NOT be distributed.
  * ``tests/`` -- Unit test scripts and data.

Prerequisites
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In order to run a development environment, docker and docker-compose are required to be
installed. For instructions on how to install these if you do not have them already,
please see INSTALL_DOCKER.rst in this directory.

We also assume that you cloned this repository from Github onto your local computer. This
should be done inside the user home directory to avoid issues where Docker does not have permission
to read and mount directories.

Unless otherwise mentioned, it is assumed that all commands mentioned in this guide
are run from the root of this cloned repository on your local computer.

On Windows, we assume that there is a Bash-like shell available (i.e. Bash under Cygwin),
with GNU make installed.

Starting a dev environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The quickest way to get started is to use a local-only environment, meaning that everything runs locally,
including a PostgreSQL server. This is much easier to setup since it doesn't require
credentials, etc. since the PostgreSQL only listens locally only. Getting this environment up and running
is described in the "Local-only environment" section below.

For larger files/tests, you might want to use an external PostgreSQL server with more resoures.
This is described in the "Remote PostgreSQL server" section below.

Local-only environment
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Building required Docker images
#########################################################

Because we require a PostgreSQL Docker image, we use our DIRBS production PostgreSQL image.
This requires the DIRBS production PostgreSQL image to be built. This can be done using
the following command:
::

    make dist; pushd dist; make -f docker/prd/Makefile; popd

After that is complete (it might take a while), you need to build the dev Docker image
using the following command:
::

    make -f docker/dev/Makefile

Creating directory for local PostgreSQL data
#########################################################

PostgreSQL needs a directory to store DB data in. To create this, run the following:
::

    mkdir ~/local_postgres

Starting dev environment
#########################################################

Now that the required Docker images are built, a dev shell can be started by running:
::

    docker-compose -f docker/compose/devenv-with-local-postgres.yml run --rm --service-ports dev-shell

After running this command, your local PostgreSQL server should be available on
localhost, port 5432 if you need to use external tools like pgAdmin or psql.

Remote PostgreSQL server
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

PostgreSQL Requirements
#########################################################

These instructions assume that you already have access to a PostgreSQL server that you
can point the software at. The minimum PostgreSQL version supported by DIRBS Core is 10.
DIRBS Core requires the installation of the PostgreSQL HLL extension to function
(https://github.com/citusdata/postgresql-hll). If using our provided Docker image for
PostgreSQL, it is already installed. If on RDS in AWS, this extension should also be optionally available.
Otherwise, you will likely need to build and install the extension. Please consult the
README in the linked Github repo for instructions on how to do this.

Creating initial PostgreSQL roles
##########################################################################

The following SQL command can be run as a superuser to create a superuser that is used purely
to create databases, install DIRBS Core base roles and create any user accounts:
::

    CREATE ROLE <username> WITH SUPERUSER LOGIN ENCRYPTED PASSWORD '<password>';

You will also want to create a separate non-superuser account that is used by the app:
::

    CREATE ROLE <username> WITH LOGIN ENCRYPTED PASSWORD '<password>';

Required environment variables for remote PostgreSQL
#########################################################

Now that you have Docker installed on your Mac OS X/Linux/Windows machine, the easiest way
to get a dev environment up and running is to first set the following environment
variables to connect to your database:

  * ``DIRBS_DB_HOST``: -- The host that the PostgreSQL database is running
    on (default: localhost)
  * ``DIRBS_DB_PORT``: -- The port that the PostgreSQL database is running
    on (default: 5432)
  * ``DIRBS_DB_DATABASE``: -- The PostgreSQL database name to connect to
    (default: XXXXXXXX)
  * ``DIRBS_DB_USER``: -- The PostgreSQL user to connect as (default: XXXXXXXX)
  * ``DIRBS_DB_PASSWORD``: -- The PostgreSQL password for DIRBS_DB_USER
    (default: XXXXXXXX)

This can be saved in your ``~/.bashrc`` or similar to avoid having to do this
every time. You can also pass environment variables on the command line to
override setting for a single invocation.

To permanently set the variables on Windows, go to Advanced System Settings ->
Environment Variables, and add the variables for the user.

Building required Docker image
#########################################################

To run the dev environment, you need to build the dev Docker image
using the following command:
::

    make -f docker/dev/Makefile

Starting dev environment
#########################################################

Now that the required Docker images are built, a dev shell can be started by running:
::

    docker-compose -f docker/compose/devenv.yml run --rm --service-ports dev-shell

Database installation guide
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This section assumes that you have a
PostgreSQL instance already running (either locally or remotely)

Installing the base roles
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

DIRBS Core requires some base roles for privilege separation. These are all marked NOLOGIN,
meaning it is not possible to login as these roles -- they are just
abstract roles that can be GRANT'ed to real users with the LOGIN privilege.

These roles are required before the database can be created or installed and are created
with the following command (run as superuser):
::

    dirbs-db install_roles

Granting role permissions to database user
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Now that roles are created, we need to assign the power user role to our non-superuser database user.
This is a simple way to ensure that our user can do everything.

Use the ``psql`` command to login to the ``postgres`` database on the PostgreSQL server
(local or remote) using the role created in Step 1 with the CREATEDB privilege.

For local-only databases from the host (not inside the dev shell):
::

    psql -h localhost -U <super_username> postgres

For remote databases, you'll need to supply the credentials to the psql command.

Once connected, the roles can be granted via the following command:
::

    GRANT dirbs_core_power_user TO <power_username>;

Creating an empty database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Now that roles are installed, we can create an empty database which is owned by the ``dirbs_core_power_user`` role.

Use the ``psql`` command to login to the ``postgres`` database on the PostgreSQL server
(local or remote) using the superuser role created.

For local-only databases from the host (not inside the dev shell):
::

    psql -h localhost -U dirbs postgres

For remote databases, you'll need to supply the credentials to the psql command.

Once connected, the database can can be created via the following command:
::

    CREATE DATABASE dirbs-local-devenv OWNER dirbs_core_power_user;

Then connect to that DB using the following psql command:
::

    \c "dirbs-local-devenv";

You'll then need to ensure that the HLL extension is installed correctly in this database:
::

    CREATE SCHEMA hll;
    GRANT USAGE ON SCHEMA hll TO dirbs_core_base;
    CREATE EXTENSION hll SCHEMA hll;

For a remote database, the database name should be unique, so you will need to pick
a unique name and make sure your DIRBS_DB_DATABASE environment variable is set to the same value.

To drop a database and re-create an empty one, you can use the following command
inside ``psql`` whilst connected to the postgres database:
::

    DROP DATABASE dirbs-local-devenv;
    CREATE DATABASE dirbs-local-devenv OWNER dirbs_core_power_user;
    \c "dirbs-local-devenv";
    CREATE SCHEMA hll;
    GRANT USAGE ON SCHEMA hll TO dirbs_core_base;
    CREATE EXTENSION hll SCHEMA hll;

Installing a database schema
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Now that an empty database is present, we need to install the DIRBS Core schema. This is done inside
the dev shell, using the following command:
::

    dirbs-db install

Upgrading a database schema
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If the database schema is bumped in code, you will need to upgrade your schema to the
code version by running migration scripts. To automatically run all migration scripts
to upgrade your schema to the required version, use the following command inside
the dev shell:
::

    dirbs-db upgrade

Checking the database schema
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If the schema is already installed check the version number to see if it compatible with the currently-installed
software
::

    dirbs-db check


Basic developer workflows in the dev shell
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following workflows assume you are in the ``/workspace`` directory after
running the ``dev-shell`` command using ``docker-compose``, as described
in the previous section.

Checking code for style errors/linting
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To lint the code using flake8, simply run
::

    make audit

Unit testing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To run the unit tests, simply run:
::

    make test

Running the API server locally
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To run the API server locally, simply run:
::

    flask run -h 0.0.0.0

The API server will then be available on localhost:5000 on the host machine

Creating a new release
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following setups show the steps required to build a new release.

Bump version number
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Version numbering for DIRBS follows `Semantic Versioning <http://semver.org/>`_

To change the release number, simply edit ``dirbs/__init__.py`` and bump the version number

It is up to the user, to then choose when to tag the software in Git and
upload the tag to the code repository.

Creating distribution
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To create the distribution (wheel, assets) for a release:
::

    make dist

All assets to be shipped will be output to the ``dist`` directory.


© 2016-2018 Qualcomm Technologies, Inc.  All rights reserved.
