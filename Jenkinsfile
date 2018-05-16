def label = "worker-${UUID.randomUUID().toString()}"

podTemplate(label: label, namespace: "jenkins", ttyEnabled: true, command: 'cat',
    containers: [
        containerTemplate(name: 'python', image: 'python:3.6.5', command: 'cat', ttyEnabled: true),
        containerTemplate(name: 'docker', image: 'docker', ttyEnabled: true, command: 'cat')
    ], 
    volumes: [
        hostPathVolume(hostPath: '/var/run/docker.sock', mountPath: '/var/run/docker.sock')
    ]
) {
    node(label) {
        stage('Clone Repo') {
            checkout scm
        }
        container('python') {
            stage('Install requirements') {
                sh 'python --version'
                sh 'pip3 install -r requirements.txt'
            }
        }
        container('docker') {
            stage('Build image') {
                sh 'docker build -t undivideddocker/celerytaskmanager .'

            }
        }
    }
}
