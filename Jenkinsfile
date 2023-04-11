pipeline {
    agent any
    
    stages {
        stage('Clone repository') {
            steps {
                git branch: 'master', url: 'https://github.com/yogi1754/pythonProject7.git'
            }
        }
        
        stage('Install dependencies') {
            steps {
                sh 'pip install pymongo pandas numpy scikit-learn matplotlib seaborn'
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
