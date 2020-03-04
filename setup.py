import os
import re
from setuptools import setup, find_packages

ROOT = os.path.dirname(__file__)
VERSION_RE = re.compile(r'''__version__ = ['"]([0-9.]+)['"]''')

requires = [
    "boto3>=1.11.13"
]


def get_version():
    init = open(os.path.join(ROOT, 'boto3_batch_utils', '__init__.py')).read()
    return VERSION_RE.search(init).group(1)


def readme():
    with open('README.md') as f:
        return f.read()


setup(name='boto3_batch_utils',
      description='A Client for managing batch interactions with AWS services',
      url='https://g-farrow.github.io/boto3_batch_utils/',
      license='GNU APGL v3',
      author='Greg Farrow',
      author_email='greg.farrow1@gmail.com',
      packages=find_packages(include=['boto3_batch_utils*']),
      version=get_version(),
      keywords='aws boto3 kinesis dynamo dynamodb sqs fifo batch',
      long_description=readme(),
      long_description_content_type='text/markdown',
      include_package_data=True,
      install_requires=requires)
