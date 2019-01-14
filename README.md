![codecov](https://codecov.io/gh/dfci/cidc-taskmanager/branch/master/graph/badge.svg)
#### Celery Taskmanager

This program is responsible for receiving messages and running tasks. It interfaces with RabbitMQ.

#### Installation

Clone git repository, then run:

```bash dockerbuild.sh```

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

#### Configuring Cromwell to Run in the Cloud

For a good introduction to the topic: http://cromwell.readthedocs.io/en/develop/tutorials/PipelinesApi101/

You will need to create a Google config file that looks something like this:

```include required(classpath("application"))

google {

  application-name = "cromwell"

  auths = [
    {
      name = "service-account"
      scheme = "service_account"
      service-account-id = "cromwell@cidc-dfci.iam.gserviceaccount.com"
      json-file = "./.google_auth.json"
    }
  ]
}

engine {
  filesystems {
    gcs {
      auth = "service-account"
    }
  }
}

backend {
  default = "JES"
  providers {
    JES {
      actor-factory = "cromwell.backend.impl.jes.JesBackendLifecycleActorFactory"
      config {
        // Google project
        project = "cidc-dfci"

        // Base bucket for workflow executions
        root = "gs://lloyd-test-pipeline/"

        // Polling for completion backs-off gradually for slower-running jobs.
        // This is the maximum polling interval (in seconds):
        maximum-polling-interval = 600

        // Optional Dockerhub Credentials. Can be used to access private docker images.
        dockerhub {
          // account = ""
          // token = ""
        }

        genomics {
          // A reference to an auth defined in the `google` stanza at the top.  This auth is used to create
          // Pipelines and manipulate auth JSONs.
          auth = "service-account"
          // Endpoint for APIs, no reason to change this unless directed by Google.
          endpoint-url = "https://genomics.googleapis.com/"
        }

        
        filesystems {
          gcs {
            // A reference to a potentially different auth for manipulating files via engine functions.
            auth = "service-account"
          }
        }
      }
    }
  }
}```


Most of this file can be used "as is", although you may want to change the "root" of the project if you don't want to run starting from inside my testing bucket. The authentication file for the service account is something we will need to distribute privately, and is not going to be checked into the repo. 
