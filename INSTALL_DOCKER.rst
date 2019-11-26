Installing Docker and Docker Compose
====================================

This document details the process to get the docker environment set up before 
the interactive dev environment can be launched. There is a section for
each platform with platform-specific instructions.

Mac OS X
###########################################################

Docker installation for Mac OS X is trivial using Docker for Mac. Installation
instructions can be found at `<https://docs.docker.com/engine/installation/mac/>`_.

Linux
###########################################################

Docker installation on a Linux host is also fairly easy, but exact instructions depend on the
Linux distribution being used. Docker also requires a minimum kernel version that might not
be available on very old Linux distributions.

Instructions for installation on various Linux distributions can be found
at `<https://docs.docker.com/engine/installation/linux/>`_.

**Please note**: For the project Makefiles to work correctly, the user must
be able to run the docker command without sudo privileges. Usually, this is done
by adding the user to a specific docker group. For instructions, please see
`<https://docs.docker.com/engine/installation/linux/ubuntulinux/#/create-a-docker-group>`_.

Windows
###########################################################

On Windows, there are few more steps required to get Docker running:

**Step 1:** To get started install docker virtual environment for Windows

`Docker Installation Instructions for Windows <https://docs.docker.com/docker-for-windows>`_

**Step 2:** Install GNU Make for Windows

If you do not have Unix-like environment like cygwin installed, you will need to install Make:

`GNU Make for Windows <http://gnuwin32.sourceforge.net/packages/make.htm>`_

Add make to the PATH variable using following command.
::

    setx PATH "%PATH%;C:\Program Files (x86)\GnuWin32\bin

Open a new command prompt session for settings to take effect.

**Step 3:** Install Virtual Box

Install Virtual Box from `<https://www.virtualbox.org/wiki/Downloads>`_.

**Step 4:** Disable Hyper-V (Windows 10 only)

  1. Open the Control Panel (icons view), and click/tap on the Programs and Features icon.
  2. Click/tap on the Turn Windows features on or off link on the left side.
  3. On the Optional Features screen, unselect Hyper-V.
  4. Select OK.
  5. Reboot the PC.
  6. If docker dialog box asks to turn on Hyper-V after reboot, click cancel.

**Step 5:** Initialize docker environment (using windows command line)

  1. Create the default docker machine:
     ::

         docker-machine create --driver virtualbox default

  2. Initialize the docker machine:
     ::

         docker-machine create --driver virtualbox default
         docker-machine start default
         docker-machine env --shell cmd default
         FOR /f "tokens=*" %i IN ('docker-machine env --shell cmd default') DO %i

Note: Step 2 above needs to be done every time a new prompt session is launched.

Docker Compose Installation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To install ``docker-compose``, simple type:
::

    sudo pip install docker-compose

On Windows exclude the `sudo` prefix while executing the above command.

For Mac OS X users
############################

You may see the following error messages when trying to run docker-compose after installed it:
::

    from six.moves import _thread
    ImportError: cannot import name _thread

To address this, we need to install python with homebrew and upgrade six module:

**Step 1:** To get started install Homebrew on OS X:

`Homebrew Installation Instructions for Mac OS X <http://brew.sh>`_

**Step 2:** Install python with Homebrew:
::

    brew install python

**Step 3:** Upgrade six module with pip:
::

    sudo pip install --upgrade six

**Step 4:** Re-install docker-compose with pip:
::

    sudo pip install --upgrade --ignore-installed docker-compose
