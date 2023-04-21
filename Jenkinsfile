pipeline {
  agent any

  stages {
    stage('Download and extract dataset') {
      steps {
        bat 'curl -O https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Gift_Card_v1_00.tsv.gz'
        bat 'gunzip amazon_reviews_us_Gift_Card_v1_00.tsv.gz'
      }
    }

    stage('Insert data into MongoDB') {
      steps {
        script {
          def client = new MongoClient()
          def db = client.getDatabase('amazon_reviews')
          def collection = db.getCollection('gift_cards')
          def tsv = new File('amazon_reviews_us_Gift_Card_v1_00.tsv')
          tsv.splitEachLine('\t') {
            fields ->
              def document = [: ]
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
          client.close()
        }
      }
    }

    stage('Load data from MongoDB into Pandas DataFrame') {
      steps {
        script {
          def df = sh(script: ""
            "
            python3 - << EOF
            import pandas as pd from pymongo
            import MongoClient client = MongoClient() db = client['amazon_reviews'] collection = db['gift_cards'] df = pd.DataFrame(list(collection.find())) client.close() print(df.to_json()) EOF ""
            ", returnStdout: true).trim()
            env.DATAFRAME_JSON = df
          }
        }
      }

      stage('Perform data cleaning and transformation') {
        steps {
          script {
            def df = new JsonSlurper().parseText(env.DATAFRAME_JSON)
            df['review_date'] = pd.to_datetime(df['review_date'])
            df['star_rating'] = pd.to_numeric(df['star_rating'], errors = 'coerce')
            df = df.dropna()
            df['log_rating'] = np.log(df['star_rating'])
            q1, q3 = np.percentile(df['log_rating'], [25, 75])
            iqr = q3 - q1
            lower_bound = q1 - (1.5 * iqr)
            upper_bound = q3 + (1.5 * iqr)
            df = df[(df['log_rating'] >= lower_bound) & (df['log_rating'] <= upper_bound)]
            env.DATAFRAME_JSON = df.to_json()
          }
        }
      }

      stage('Insert data into SQL Server') {
        steps {
          script {
            def df = new JsonSlurper().parseText(env.DATAFRAME_JSON)
            def connectionString = "Driver=SQL Server;Server=localhost\\SQLEXPRESS;Database=master;Trusted_Connection=yes;"
            def connection = Sql.newInstance(connectionString)
            connection.execute(""
              "
              CREATE TABLE gift_card_reviews(
                marketplace VARCHAR(255),
                customer_id VARCHAR(255),
                review_id VARCHAR(255),
                product_id VARCHAR(255),
                product_parent VARCHAR(255),
                product_title VARCHAR(255),
                product_category VARCHAR(255),
                star_rating FLOAT,
                helpful_votes INT,
                total_votes INT,
                vine VARCHAR(255),
                verified_purchase VARCHAR(255),
                review_headline VARCHAR(255),
                review_body VARCHAR(MAX),
                review_date DATETIME,
                log_rating FLOAT
              )
              ""
              ")
              df.each {
                row ->
                  connection.execute(""
                    "
                    INSERT INTO gift_card_reviews(
                      marketplace,
                      customer_id,
                      review_id,
                      product_id,
                      product_parent,
                      product_title,
                      product_category,
                      star_rating,
                      helpful_votes,
                      total_votes,
                      vine,
                      verified_purchase,
                      review_headline,
                      review_body,
                      review_date,
                      log_rating
                    ) VALUES( ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? )
                    ""
                    ", [
                    row.marketplace,
                    row.customer_id,
                    row.review_id,
                    row.product_id,
                    row.product_parent,
                    row.product_title,
                    row.product_category,
                    row.star_rating,
                    row.helpful_votes,
                    row.total_votes,
                    row.vine,
                    row.verified_purchase,
                    row.review_headline,
                    row.review_body,
                    row.review_date,
                    row.log_rating
                  ])
            }

            connection.close()
          }
        }
      }
    }
