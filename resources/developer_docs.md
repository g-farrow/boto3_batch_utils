# Developer Documentation
## Linting
Linting is checked by [flake8](https://flake8.pycqa.org/en/latest/). This can be run from the root of the repository
by running `flake8 .`.

Flake8 refers to configuration within `.flake8`

## Coverage
Code coverage is checked by the [Coverage](https://coverage.readthedocs.io/en/coverage-5.0.3/) library.

Coverage is configured within `.coveragerc`.

## Unit Tests
Unit tests are stored within `/tests/unit_tests`. Tests are run using the nose tool: `nosetests tests/unit_tests`

Unit test coverage must be a minimum of 85%.

## Integration Tests
Integration tests are stored within `tests/integration_tests`. Tests are run using the nose tool: 
`nosetests tests/integration_tests`

## Versions
The project follow Semantic Versioning in the format `X.X.X`:
* **Major**: Release contains removed functionality or other breaking changes
* **Minor**: Functionality has been changed but changes are not breaking
* **Patch**: A defect fix or other small change which will not adversely affect users

The tool [`python-semantic-release`](https://github.com/relekang/python-semantic-release) is used to manage version 
numbers.

Version number is stored and maintained in `boto3_batch_utils/__init__.py:__version__`. Versions are managed on the
`develop` branch only:
* Code changes are made to a feature branch or `develop`.
* Once ready `master` is merged back into `develop`.
* Version is increased using `semantic-release version --<major|minor|patch>` (which is configured in `setup.cfg`).
* Pull Request made from `develop` branch to `master`.

## Automation, CI/CD
All branches utilise GitHub Actions to automate linting, unit tests and integration tests. These are run automatically
upon a `push`.

In addition to the above, the `master` branch also automatically packages and publishes the library to 
[PyPi](https://pypi.org/project/boto3-batch-utils/). It does so using the assigned version (see above).