from distutils.core import setup
from setuptools import find_packages
import os

quay_dir = os.path.dirname(os.path.realpath(__file__))
requirementPath = quay_dir + '/requirements.txt'
install_requires = []
if os.path.isfile(requirementPath):
    with open(requirementPath) as f:
        for line in f.read().splitlines():
            if not line.startswith("git"):
                install_requires.append(line)
print(install_requires)
setup(name='quay',
      version='3.6',
      description='Quay application',
      author='quay team',
      author_email='',
      url='https://github.com/bcaton85/quay',
      packages=find_packages(),
      install_requires=install_requires,
     )
