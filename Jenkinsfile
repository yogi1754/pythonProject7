pipeline {
    agent any
    
    stages {
        stage('Clone repository') {
            steps {
                git branch: 'master', url: 'https://github.com/yogi1754/pythonProject7.git'
            }
        }
        
        stage('Run script') {
            steps {
                sh 'main.py'
            }
        }
        
        stage('Publish results') {
            steps {
                archiveArtifacts artifacts: 'amazon_reviews_us_Gift_Card_v1_00.tsv.gz'
                publishHTML(target: [
                    allowMissing: false,
                    alwaysLinkToLastBuild: false,
                    keepAll: true,
                    reportDir: 'reports',
                    reportFiles: 'index.html',
                    reportName: 'Amazon Reviews'
                ])
            }
        }
    }
}
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
