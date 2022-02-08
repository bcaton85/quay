from distutils.core import setup
from setuptools import find_packages
import os

packages = [ "prometheus_client", "alembic", "Authlib", "bitmath", "boto3", "bcrypt", "botocore", "cachetools", "cryptography", "Deprecated", "elasticsearch", "Flask", "hashids", "jsonschema", "keystoneauth1", "peewee", "pymemcache", "PyYAML", "redis", "rehash", "six", "SQLAlchemy", "stripe", "tldextract", "toposort", "tzlocal", "beautifulsoup4", "bintrees", "geoip2", "gevent", "greenlet", "gunicorn", "Jinja2", "mixpanel", "netaddr", "psutil", "PyJWT", "pyOpenSSL", "raven", "redlock", "requests", "Werkzeug", "xhtml2pdf" ]

quay_dir = os.path.dirname(os.path.realpath(__file__))
requirementPath = quay_dir + '/requirements.txt'
install_requires = []
if os.path.isfile(requirementPath):
    with open(requirementPath) as f:
        for line in f.read().splitlines():
            if not line.startswith("git") and line.split('==')[0] in packages:
                install_requires.append(line)

setup(name='quay',
      version='3.6',
      description='Quay application',
      author='quay team',
      author_email='',
      url='https://github.com/bcaton85/quay',
      packages=['data','data.model','data.model.oci','util','util.security','util.metrics','image','image.docker', 'image.docker.schema2', 'image.shared','digest'],
      install_requires=install_requires,
     )
