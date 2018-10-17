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
      deploy = "${UUID.randomUUID().toString()}"
  }
  stages {
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
    stage('Docker deploy (staging)') {
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
          sh '''helm upgrade celery-taskmanager cidc/celery-taskmanager --set imageSHA=$(gcloud container images list-tags --format='get(digest)' --filter='tags:staging' gcr.io/cidc-dfci/celery-taskmanager) --set image.tag=staging --tls'''
        }
      }
    }
  }
}
