def label = "worker-${UUID.randomUUID().toString()}"

podTemplate(label: label, namespace: "jenkins", ttyEnabled: true,
    containers: [
        containerTemplate(name: 'python', image: 'python:3.6.5', command: 'cat', ttyEnabled: true),
        containerTemplate(name: 'docker', image: 'docker', ttyEnabled: true, command: 'cat')
    ], 
    volumes: [
        hostPathVolume(hostPath: '/var/run/docker.sock', mountPath: '/var/run/docker.sock')
    ]
) {
    node(label) {
        stage('Run unit tests') {
            container('python') {
                checkout scm
                sh '''
                python --version
                ls
                pip3 install -r requirements.txt
                which nose2
                nose2 --verbose
                '''
            }
        }
        container('docker') {
            stage('Build image') {
                sh 'docker build -t undivideddocker/celerytaskmanager .'

            }
        }
    }
}
