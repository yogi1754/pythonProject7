pipeline {
    agent any

    stages {
        stage('Clone Repository') {
            steps {
                git branch: 'master', url: 'https://github.com/yogi1754/pythonProject7.git'
            }
        }

        stage('Run Python Script') {
            steps {
                sh 'python main.py'
            }
        }

        stage('Push Results to GitHub') {
            steps {
                git branch: 'master', url: 'https://github.com/yogi1754/pythonProject7.git'
                sh 'cp -r ./output/* ./new-project/'
                sh 'cd ./new-project && git add . && git commit -m "Update results" && git push'
            }
        }
    }
}
