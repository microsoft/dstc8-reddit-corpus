from setuptools import setup, find_packages
from dstc8_reddit import __version__


with open('README.md', 'r') as fh:
  long_description = fh.read()

INSTALL_REQUIRES = [
  'click>=7',
  'luigi>=2.8.6,<2.9',
  'numpy',
  'pydantic>=0.26,<1.0',
  'python-rapidjson',
  'pyyaml',
  'requests',
]

TEST_REQUIRES = [
  'pytest>=4',
  'pytest-flake8',
  'pytest-env',
  'flake8',
]

DEV_REQUIRES = [
]


setup(
  name='dstc8-reddit',
  version=__version__,
  author='Adam Atkinson',
  author_email='adam.atkinson@microsoft.com',
  description='Pushshift Reddit Data Processor for DSTC 8',
  long_description=long_description,
  url='https://github.com/mirosoft/dstc8-reddit',
  packages=find_packages(),
  classifiers=[
      'Programming Language :: Python :: 3',
  ],
  python_requires='>=3.6',
  install_requires=INSTALL_REQUIRES,
  tests_require=TEST_REQUIRES,
  extras_require=dict(
    test=TEST_REQUIRES,
    dev=DEV_REQUIRES + TEST_REQUIRES,
  )
)
