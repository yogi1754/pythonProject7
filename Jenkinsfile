pipeline {
    agent any

    options {
        skipStagesAfterUnstable()
        timestamps()
        buildDiscarder(logRotator(numToKeepStr:'5'))
        timeout(time: 1, unit: 'HOURS')
        quietPeriod(5)
    }

    stages {
        stage('Clone Repository') {
            steps {
                git branch: 'master', url: 'https://github.com/yogi1754/pythonProject7.git'
            }
        }

        stage('Run Python Script') {
            steps {
                sh 'nohup python game.py > game.out 2>&1 &'
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
