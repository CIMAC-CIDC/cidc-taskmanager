pipeline {
  agent {
    kubernetes {
      label 'helm-docker'
      defaultContainer 'jnlp'
      serviceAccount 'helm'
      yaml """
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: docker
    image: docker:latest
    command:
    - cat
    tty: true
    volumeMounts:
    - mountPath: /var/run/docker.sock
      name: docker-volume
  - name: python
    image: python:3.6.5
    command:
    - cat
    tty: true
  - name: gcloud
    image: gcr.io/cidc-dfci/gcloud-helm:latest
    command:
    - cat
    tty: true
  volumes:
  - name: docker-volume
    hostPath: 
      path: /var/run/docker.sock
"""
    }
  }
  environment {
      GOOGLE_APPLICATION_CREDENTIALS = credentials('google-service-account')
      CODECOV_TOKEN = credentials('cidc-taskmanager-codecov-token')
      CA_CERT_PEM = credentials("ca.cert.pem")
      HELM_CERT_PEM = credentials("helm.cert.pem")
      HELM_KEY_PEM = credentials("helm.key.pem")
  }
  stages {
    stage('Run unit tests') {
      steps {
        container('python') {
          checkout scm
          sh 'pip3 install -r requirements.txt'
          sh 'pytest --html=celery_tests.html'
          sh 'pytest --cov-report xml:coverage.xml --cov ./'
          sh 'curl -s https://codecov.io/bash | bash -s - -t ${CODECOV_TOKEN}'
        }
      }
    }
    stage('Checkout SCM') {
      steps {
        container('docker') {
          checkout scm
        }
      }
    }
    stage('Docker login') {
      steps {
        container('docker') {
          sh 'cat ${GOOGLE_APPLICATION_CREDENTIALS} | docker login -u _json_key --password-stdin https://gcr.io'
        }
      }
    }
    stage('Docker build (master)') {
      when {
        branch 'master'
      }
      steps {
        container('docker') {
          sh 'docker build -t celery-taskmanager .'
          sh 'docker tag celery-taskmanager gcr.io/cidc-dfci/celery-taskmanager:production'
          sh 'docker push gcr.io/cidc-dfci/celery-taskmanager:production'
        }
      }
    }
    stage('Docker build (staging)') {
      when {
        branch 'staging'
      }
      steps {
        container('docker') {
          sh 'docker build -t celery-taskmanager . --no-cache'
          sh 'docker tag celery-taskmanager gcr.io/cidc-dfci/celery-taskmanager:staging'
          sh 'docker push gcr.io/cidc-dfci/celery-taskmanager:staging'
        }
      }
    }
    stage('Upload report (dev)') {
      when {
        not {
          anyOf {
            branch "master";
            branch "staging"
          }
        }
      }
      steps {
        container('gcloud') {
          sh 'gsutil cp celery_tests.html gs://cidc-test-reports/celery/dev'
        }
      }
    }
    stage('Upload report (staging)') {
      when {
        branch 'staging'
      }
      steps {
        container('gcloud') {
          sh 'gsutil cp celery_tests.html gs://cidc-test-reports/celery/staging'
        }
      }
    }
    stage('Upload report (master)') {
      when {
        branch 'master'
      }
      steps {
        container('gcloud') {
          sh 'gsutil cp celery_tests.html gs://cidc-test-reports/celery/master'
        }
      }
    }
    stage('Helm deploy (staging)') {
      when {
          branch 'staging'
      }
      steps {
        container('gcloud') {
          sh 'gcloud container clusters get-credentials cidc-cluster-staging --zone us-east1-c --project cidc-dfci'
          sh 'helm init --client-only'
          sh 'cat ${CA_CERT_PEM} > $(helm home)/ca.pem'
          sh 'cat ${HELM_CERT_PEM} > $(helm home)/cert.pem'
          sh 'cat ${HELM_KEY_PEM} > $(helm home)/key.pem'
          sh 'helm repo add cidc "http://${CIDC_CHARTMUSEUM_SERVICE_HOST}:${CIDC_CHARTMUSEUM_SERVICE_PORT}" '
          sh 'sleep 10'
          sh 'helm fetch cidc/celery-taskmanager --version=0.1.0-staging --untar'
          sh 'helm upgrade celery-taskmanager celery-taskmanager/ -f celery-taskmanager/values_staging.yaml --set imageSHA=$(gcloud container images list-tags --format="get(digest)" --filter="tags:staging" gcr.io/cidc-dfci/celery-taskmanager) --set image.tag=staging --tls'
          sh 'sleep 10'
          sh 'kubectl wait pod -l app=celery-taskmanager --for=condition=Ready --timeout=180s'
        }
      }
    }
  }
}
