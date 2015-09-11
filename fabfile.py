from fabric.api import local

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

pip_repo = '***REMOVED***'
dist_dir = 'dist'


def prep():
    local("pip install pip2pi")


def package():
    local("python setup.py sdist")


def deploy():
    name = local("python setup.py --name", capture=True)
    ver = local("python setup.py --version", capture=True)
    sdist_name = '{}-{}.tar.gz'.format(name, ver)
    local("pip2pi {} {}/{}".format(pip_repo, dist_dir, sdist_name))


def publish():
    prep()
    package()
    deploy()