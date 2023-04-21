pipeline {
    agent any

    stages {
        stage('Clone repository') {
            steps {
                git branch: 'master', url: 'https://github.com/yogi1754/pythonProject7.git'
            }
        }
        stage('Create virtual environment') {
            steps {
                sh 'py -m venv env'
            }
        }
        stage('Activate virtual environment and install dependencies') {
            steps {
                bat 'env\\Scripts\\activate.bat && py -m pip install -r requirements.txt'
            }
        }
        stage('Run script') {
            steps {
                bat 'start cmd /k "env\\Scripts\\activate.bat && py game.py"'
            }
        }
    }
}
