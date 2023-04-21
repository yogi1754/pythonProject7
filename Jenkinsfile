pipeline {
    agent any
    
    stages {
stage('Download and insert dataset into MongoDB') {
            steps {
                script {                   
                    import com.mongodb.MongoClient
                    import groovy.json.JsonSlurperClassic
                    // Connect to MongoDB
                    def mongodbServer = "mongodb://localhost:27017"
                    def mongoClient = new MongoClient(mongodbServer)
                    def databaseName = 'amazon_reviews'
                    def collectionName = 'gift_cards'
                    def db = mongoClient.getDatabase(databaseName)
                    def collection = db.getCollection(collectionName)
                    
                    // Download and extract the dataset
                    def url = 'https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Gift_Card_v1_00.tsv.gz'
                    def filename = 'amazon_reviews_us_Gift_Card_v1_00.tsv.gz'
                    sh "curl -L -o ${filename} ${url}"
                    sh "gzip -d ${filename}"
                    def tsv = new File('amazon_reviews_us_Gift_Card_v1_00.tsv')
                    
                    // Insert data into MongoDB
                    tsv.splitEachLine('\t') { fields ->
                        def document = [:]
                        document['marketplace'] = fields[0]
                        document['customer_id'] = fields[1]
                        document['review_id'] = fields[2]
                        document['product_id'] = fields[3]
                        document['product_parent'] = fields[4]
                        document['product_title'] = fields[5]
                        document['product_category'] = fields[6]
                        document['star_rating'] = fields[7] as int
                        document['helpful_votes'] = fields[8] as int
                        document['total_votes'] = fields[9] as int
                        document['vine'] = fields[10]
                        document['verified_purchase'] = fields[11]
                        document['review_headline'] = fields[12]
                        document['review_body'] = fields[13]
                        document['review_date'] = fields[14]
                        collection.insertOne(document)
                    }
                    
                    // Close MongoDB connection
                    mongoClient.close()
                }
   
          def df = bat(script: """
            python3 - << EOF
            import pandas as pd from pymongo
            import MongoClient 
            client = MongoClient() 
            db = client['amazon_reviews'] 
            collection = db['gift_cards'] 
            df = pd.DataFrame(list(collection.find())) 
            client.close() 
            print(df.to_json()) 
            EOF 
            """, returnStdout: true).trim()
            env.DATAFRAME_JSON = df
        
            def df1 = JsonSlurper().parseText(env.DATAFRAME_JSON)
            df1['review_date'] = pd.to_datetime(df['review_date'])
            df1['star_rating'] = pd.to_numeric(df['star_rating'], errors = 'coerce')
            df1 = df1.dropna()
            df1['log_rating'] = np.log(df['star_rating'])
            q1 = np.percentile(df1['log_rating'], 25)
            q3 = np.percentile(df1['log_rating'], 75)
            iqr = q3 - q1
            lower_bound = q1 - (1.5 * iqr)
            upper_bound = q3 + (1.5 * iqr)
            df1 = df1[(df1['log_rating'] >= lower_bound) & (df1['log_rating'] <= upper_bound)]
            env.DATAFRAME_JSON = df1.to_json()

            def df2 = JsonSlurper().parseText(env.DATAFRAME_JSON)
            def connectionString = "Driver=SQL Server;Server=localhost\\SQLEXPRESS;Database=master;Trusted_Connection=yes;"
            def connection = Sql.newInstance(connectionString)
            connection.execute("""
                CREATE TABLE gift_card_reviews (
                    marketplace VARCHAR(255),
                    customer_id VARCHAR(255),
                    review_id VARCHAR(255) PRIMARY KEY,
                    product_id VARCHAR(255),
                    product_parent VARCHAR(255),
                    product_title VARCHAR(255),
                    product_category VARCHAR(255),
                    star_rating INT,
                    helpful_votes INT,
                    total_votes INT,
                    vine VARCHAR(255),
                    verified_purchase VARCHAR(255),
                    review_headline VARCHAR(255),
                    review_body TEXT,
                    review_date DATE
                )
            """)
            df2.each { row ->
                def values = []
                values << row['marketplace']
                values << row['customer_id']
                values << row['review_id']
                values << row['product_id']
                values << row['product_parent']
                values << row['product_title']
                values << row['product_category']
                values << row['star_rating']
                values << row['helpful_votes']
                values << row['total_votes']
                values << row['vine']
                values << row['verified_purchase']
                values << row['review_headline']
                values << row['review_body']
                values << row['review_date']
                def sql = "INSERT INTO gift_card_reviews VALUES (" + values.collect { "'${it}'" }.join(',') + ")"
                connection.execute(sql)
            }
            connection.close(message: 'Closing SQL Server connection')
        }
    }
}
}

