pipeline {
    agent any
    
    stages {
        stage('Clone Repository') {
            steps {
                git branch: 'master', url: 'https://github.com/yogi1754/pythonProject7.git'
            }
        }
        
        stage('Install Dependencies') {
            steps {
                bat 'pip install pymongo'
            }
        }

        stage('Run Python Script') {
            steps {
                bat 'game.py'
            }
        }              

        stage('Push Results to GitHub') {
            steps {
                git branch: 'master', url: 'https://github.com/yogi1754/pythonProject7.git'
                bat 'cp -r ./output/* ./new-project/'
                bat 'cd ./new-project && git add . && git commit -m "Update results" && git push'
            }
        }
    }
}
