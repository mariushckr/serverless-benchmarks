from setuptools import setup
from glob import glob

with open('requirements.txt') as f:
    requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]

setup(
    name='function',
    install_requires=requirements,
    packages=['function'],
    package_dir={'function': '.'},
    package_data={'function': glob('**', recursive=True)},
)