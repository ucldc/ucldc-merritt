import os
from setuptools import setup

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name = "UCLDC Merritt",
    version = "0.0.1",
    description = ("UCLDC code for pushing shared DAMS content into Merritt"),
    long_description=read('README.md'),
    author='Barbara Hui',
    author_email='barbara.hui@ucop.edu',
    dependency_links=[
        'https://github.com/ucldc/pynux/archive/master.zip#egg=pynux',
        'https://github.com/barbarahui/nuxeo-calisphere/archive/master.zip#egg=UCLDC-Deep-Harvester'
    ],
    install_requires=[
        'lxml',
        'pynux',
        'python-dateutil',
        'UCLDC-Deep-Harvester',
        'boto3'
    ],
    test_suite='tests'
)
