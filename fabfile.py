from fabric.api import *

""" 
    Overview
    ========

    This fabric file automates the process of pip packaging and
    deploying your new pip package to our private pip repository.

    Requirements
    ------------

    - Must have fabric installed via `pip install fabric`
    - Must have your setup.py working and up to date.  Make sure
      it works by running `python setup.py test` or do a test install
      via `python setup.py install` inside a virtualenv.

    Deploying
    ---------

    Run `fab publish` for a one step pip package deploy!
"""


def prep():
    local("pip install pip2pi")


def package():
    local("python setup.py sdist")


def deploy(pip_repo):
    name = local("python setup.py --name", capture=True)
    ver = local("python setup.py --version", capture=True)
    sdist_name = '{}-{}.tar.gz'.format(name, ver)
    local("pip2pi {} dist/{}".format(pip_repo, sdist_name))


def publish():
    prep()
    package()
    deploy()