def label = "worker-${UUID.randomUUID().toString()}"

podTemplate(label: label, namespace: "jenkins", ttyEnabled: true, command: 'cat', containers: [
    containerTemplate(name: 'celery', image: 'undivideddocker/celery-taskmanager', ttyEnabled: true, command: 'cat')
]) {
    node(label) {
        stage('Run Unit Tests') {
          container('celery') {
              checkout scm
              sh 'nose2'
          }
        }
    }
}
