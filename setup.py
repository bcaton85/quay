from distutils.core import setup
import os

quay_dir = os.path.dirname(os.path.realpath(__file__))
requirementPath = quay_dir + '/requirements.txt'
install_requires = []
if os.path.isfile(requirementPath):
    with open(requirementPath) as f:
        for line in f.read().splitlines():
            if not line.startswith("git"):
                install_requires.append(line)

setup(name='quay',
      version='3.6',
      description='Quay application',
      author='quay team',
      author_email='',
      url='https://github.com/bcaton85/quay',
      packages=['data','data.model'],
      install_requires=install_requires,
     )
