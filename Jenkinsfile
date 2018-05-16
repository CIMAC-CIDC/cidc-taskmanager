def label = "worker-${UUID.randomUUID().toString()}"

podTemplate(label: label, namespace: "jenkins", ttyEnabled: true, command: 'cat', containers: [
    containerTemplate(name: 'docker', image: 'docker', ttyEnabled: true, command: 'cat')
], volumes: [hostPathVolume(hostPath: '/var/run/docker.sock', mountPath: '/var/run/docker.sock')])
) {
    node(label) {
        container('docker') {
            def app
            stage('Clone Repo') {
                checkout scm
            }
            stage('Build image') {
                app = docker.build("undivideddocker/celery-taskmanager")
            }
            stage('Test Image') {
                app.inside {
                    sh "nose2"
                }
            }
        }
    }
}
