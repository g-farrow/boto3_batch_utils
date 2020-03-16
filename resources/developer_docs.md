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

Version number is stored and maintained in `__init__.py:` 