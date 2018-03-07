from setuptools import setup, find_packages


def get_version_number():
    with open('version.cfg', "r") as v:
        current_version = v.readlines()[0]
    user_input = input("Current version is '{}' please enter a new version number now, "
                       "or press [enter] to use the existing version number:\n".format(current_version))
    version = user_input if user_input else current_version
    print("Packaging a version '{}'".format(version))
    if version != current_version:
        with open('version.cfg', 'w') as v:
            v.write(version)
        return version
    else:
        return current_version


def readme():
    with open('README.rst') as f:
        return f.read()


setup(name='boto3_batch_utils',
      description='',
      url='https://github.com/g-farrow/boto3_batch_utils',
      license='GNU APGL v3',
      author='Greg Farrow',
      author_email='greg.farrow1@gmail.com',
      find_packages=find_packages(),
      version=get_version_number(),
      keywords='aws boto3 kinesis dynamo dynamodb batch',
      long_description=readme(),
      include_package_data=True,
      install_requires=[
          'boto3'
      ])
