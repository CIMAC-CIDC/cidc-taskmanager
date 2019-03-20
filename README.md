| Branch  | Coverage                                                                                                                                                          | Codacy                                                                                                                                                                                                                                                                           | Code Style                                                                                                        | License                                                                                                     |
| ------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| Master  | [![codecov](https://codecov.io/gh/CIMAC-CIDC/cidc-taskmanager/branch/master/graph/badge.svg)](https://codecov.io/gh/CIMAC-CIDC/cidc-taskmanager/branch/master/)   | [![Codacy Badge](https://api.codacy.com/project/badge/Grade/dd9ef5c1ce4346e898c6a2a2752370b9)](https://www.codacy.com/app/CIMAC-CIDC/cidc-taskmanager?utm_source=github.com&utm_medium=referral&utm_content=CIMAC-CIDC/cidc-taskmanager&utm_campaign=Badge_Grade?branch=master)  | [![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black) | [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) |
| Staging | [![codecov](https://codecov.io/gh/CIMAC-CIDC/cidc-taskmanager/branch/staging/graph/badge.svg)](https://codecov.io/gh/CIMAC-CIDC/cidc-taskmanager/branch/staging/) | [![Codacy Badge](https://api.codacy.com/project/badge/Grade/dd9ef5c1ce4346e898c6a2a2752370b9)](https://www.codacy.com/app/CIMAC-CIDC/cidc-taskmanager?utm_source=github.com&utm_medium=referral&utm_content=CIMAC-CIDC/cidc-taskmanager&utm_campaign=Badge_Grade?branch=staging) | [![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black) | [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) |

#### Celery Taskmanager

This service handles most of the backend logic for the CIDC system. It can be issued commands by the API through AMQP, and has scheduled tasks that it will perform periodically. Uses the Celery library to manage a pool of workers. This service is designed primarly to be run on a kubernetes cluster via a helm chart.

#### Image Build

The build is two-part. First build the Dockerfile named `BaseImage`, then build the Dockerfile named `Dockerfile`.
The second Dockerfile imports the image built by BaseImage. BaseImage only needs to be rebuilt when the dependencies it installs need to be updated.

#### Running Tests

To run unit tests:

    pipenv shell
    pytest

To generate an XML for code coverage plugins:

    pipenv shell
    pytest --cov-report xml:coverage.xml --cov ./

To generate an HTML output:
  
 pipenv shell
pytest --html=report.html
