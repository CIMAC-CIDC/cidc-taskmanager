| Branch | Coverage |
| --- | --- |
| Master | [![codecov](https://codecov.io/gh/CIMAC-CIDC/cidc-taskmanager/branch/master/graph/badge.svg)](https://codecov.io/gh/CIMAC-CIDC/cidc-taskmanager/branch/master/) |
| Staging | [![codecov](https://codecov.io/gh/CIMAC-CIDC/cidc-taskmanager/branch/staging/graph/badge.svg)](https://codecov.io/gh/CIMAC-CIDC/cidc-taskmanager/branch/staging/) |
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
