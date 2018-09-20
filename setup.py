from setuptools import setup, find_packages


def get_version_number():
    with open('version.cfg', "r") as v:
        version = v.readlines()[0]
    return version


def readme():
    with open('README.md') as f:
        return f.read()


setup(name='boto3_batch_utils',
      description='',
      url='https://github.com/g-farrow/boto3_batch_utils',
      license='GNU APGL v3',
      author='Greg Farrow',
      author_email='greg.farrow1@gmail.com',
      packages=find_packages(exclude=['tests*']),
      version=get_version_number(),
      keywords='aws boto3 kinesis dynamo dynamodb batch',
      long_description=readme(),
      include_package_data=True,
      install_requires=[])
