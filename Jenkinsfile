pipeline {
  agent any
  
  tools {
    maven 'Maven'
    jdk 'Java'
  }

  stages {
    stage('Install Packages') {
      steps {
        sh 'mvn install:install-file -Dfile=./ojdbc8.jar -DgroupId=com.oracle -DartifactId=ojdbc8 -Dversion=19.3 -Dpackaging=jar'
        sh 'python3 -m pip install pymongo pandas numpy scikit-learn matplotlib seaborn'
      }
    }

    stage('Download and Extract Dataset') {
      steps {
        sh '''
        curl -O https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Gift_Card_v1_00.tsv.gz 
        gzip -d amazon_reviews_us_Gift_Card_v1_00.tsv.gz
        tail -n +1010 amazon_reviews_us_Gift_Card_v1_00.tsv > amazon_reviews_us_Gift_Card_v1_00_limit_1010.tsv 
        '''
      }
    }

    stage('Data Processing') {
      steps {
        script {
          // Import required modules and libraries
          def pymongo = library('pymongo')
          def pandas = library('pandas')
          def numpy = library('numpy')
          def sklearn = library('sklearn')
          def matplotlib = library('matplotlib')
          def seaborn = library('seaborn')

          // Connect to MongoDB
          def client = pymongo.MongoClient()
          def database_name = 'amazon_reviews'
          def collection_name = 'gift_cards'
          def db = client.getDatabase(database_name)
          def collection = db.getCollection(collection_name)

          // Download and extract the dataset
          def url = 'https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Gift_Card_v1_00.tsv.gz'
          def filename = 'amazon_reviews_us_Gift_Card_v1_00.tsv.gz'
          sh "curl -O $url"
          sh "gzip -d $filename"
          sh "tail -n +1010 amazon_reviews_us_Gift_Card_v1_00.tsv > amazon_reviews_us_Gift_Card_v1_00_limit_1010.tsv"

          // Process the dataset and insert into MongoDB
          def file = new File('amazon_reviews_us_Gift_Card_v1_00_limit_1010.tsv')
          def rows = file.readLines().drop(1) // skip header row
          for (int i = 0; i < rows.size(); i++) {
            def row = rows[i].split('\t')
            def document = BasicDBObject()
            document.put('marketplace', row[0])
            document.put('customer_id', row[1])
            document.put('review_id', row[2])
            document.put('product_id', row[3])
            document.put('product_parent', row[4])
            document.put('product_title', row[5])
            document.put('product_category', row[6])
            document.put('star_rating', row[7])
            document.put('helpful_votes', row[8])
            document.put('total_votes', row[9])
            document.put('vine', row[10])
            document.put('verified_purchase', row[11])
            document.put('review_headline', row[12])
            document.put('review_body', row[13])
            document.put('review_date', row[14])
            collection.insertOne(pandas.DataFrame(document))
            if (i == 1000) { // insert only 1000 documents for testing
              break
            }
          }
    
          steps {
            script {
              
        // Load data from MongoDB into a Pandas DataFrame
         rome = pd.DataFrame(list(documents))

        // Clean and normalize data
        rome = rome.drop("_id")
        rome['review_date'] = pd.to_datetime(rome['review_date'])
        rome['star_rating'] = pd.to_numeric(rome['star_rating'], errors = 'coerce')
        rome = rome.dropna()

        // Perform data transformation
        rome['log_rating'] = np.log(rome['star_rating'])

        // Handle outliers
        def q1, q3 = np.percentile(rome['log_rating'], [25, 75])
        def iqr = q3 - q1
        def lower_bound = q1 - (1.5 * iqr)
        def upper_bound = q3 + (1.5 * iqr)
        rome = rome[(rome['log_rating'] >= lower_bound) & (rome['log_rating'] <= upper_bound)]

        // Connect to SQL Server
        def cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=YOGESH\\SQLEXPRESS;DATABASE=database_name')
        def db_cursor = cnxn.cursor()

        // Create SQL table for data
        cursor.execute('CREATE TABLE gift_card_reviews (marketplace varchar(255), customer_id varchar(255), review_id varchar(255), product_id varchar(255), product_parent varchar(255), product_title varchar(255), product_category varchar(255), star_rating int, helpful_votes int, total_votes int, vine varchar(255), verified_purchase varchar(255), review_headline varchar(255), review_body varchar(max), review_date date, log_rating float)')

        // Insert data into SQL table
        for (def row: rome.iterrows()) {
          cursor.execute('INSERT INTO gift_card_reviews (marketplace, customer_id, review_id, product_id, product_parent, product_title, product_category, star_rating, helpful_votes, total_votes, vine, verified_purchase, review_headline, review_body, review_date, log_rating) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            row['marketplace'], row['customer_id'], row['review_id'], row['product_id'], row['product_parent'], row['product_title'], row['product_category'], row['star_rating'], row['helpful_votes'], row['total_votes'], row['vine'], row['verified_purchase'], row['review_headline'], row['review_body'], row['review_date'], row['log_rating'])}
        cnxn.commit()

        // Close the SQL connection
        cursor.close()
        cnxn.close()

        // Visualize the data using a scatter plot
        plt.scatter(rome['review_date'], rome['log_rating'])
        plt.title("Logarithm of Rating Over Time")
        plt.xlabel("review_date")
        plt.ylabel("Logarithm of Rating")
        plt.savefig('https://github.com/yogi1754/pythonProject7.git/Logarithm of Rating Over Time.png')
        plt.close()

        // Create a bar chart of the count of reviews by gift card category
        category_counts = rome.groupby('product_category')['review_id'].count()
        plt.bar(category_counts.index, category_counts.values)
        plt.title("Number of Reviews by Gift Card Category")
        plt.xlabel("Gift Card Category")
        plt.ylabel("Number of Reviews")
        plt.xticks(rotation = 90)
        plt.savefig('https://github.com/yogi1754/pythonProject7.git/number_of_reviews_by_gift_card_category.png')
        plt.close()

        // Create a scatter plot matrix of the numerical variables in the dataset
        sns.pairplot(rome.select_dtypes(include = [np.number]))
        plt.suptitle("Scatter Plot Matrix")
        plt.savefig('https://github.com/yogi1754/pythonProject7.git/scatter_plot_matrix.png')
        plt.close()

        // Create a time series plot of the average star rating per month
        df['year_month'] = rome['review_date'].dt.to_period('M')
        monthly_avg_rating = rome.groupby('year_month')['star_rating'].mean()
        plt.plot(monthly_avg_rating.index.to_timestamp(), monthly_avg_rating.values)
        plt.title("Average Star Rating per Month")
        plt.xlabel("Month")
        plt.ylabel("Average Star Rating")
        plt.savefig('https://github.com/yogi1754/pythonProject7.git/average_star_rating_per_month.png')
        plt.close()

        // Fit a linear regression model
        X = rome[['review_date']].astype(int)
        y = rome['star_rating']
        model = LinearRegression().fit(X, y)

        // Create a histogram of star ratings
        plt.hist(rome['star_rating'], bins = 5)
        plt.title("Histogram of Star Ratings")
        plt.xlabel("Star Ratings")
        plt.ylabel("Count")
        plt.savefig('https://github.com/yogi1754/pythonProject7.git/Histogram of Star Ratings.png')
        plt.close()

        // Create a bar chart of the count of reviews by gift card category
        def category_counts = rome.groupby('product_category')['review_id'].count()
        plt.bar(category_counts.index, category_counts.values)
        plt.title("Number of Reviews by Gift Card Category")
        plt.xlabel("Gift Card Category")
        plt.ylabel("Number of Reviews")
        plt.xticks(rotation = 90)
        plt.savefig('https://github.com/yogi1754/pythonProject7.git/number_of_reviews_by_gift_card_category.png')
        plt.close()

        // Create a violinplot of the star ratings and save the figure
        sns.violinplot(x = 'star_rating', data = df)
        plt.title("Violinplot of Star Ratings")
        plt.savefig('https://github.com/yogi1754/pythonProject7.git/violinplot_star_ratings.png')

        // Close the MongoDB connection
        client.close()
        }
      }
    }

    stage('Push Results to GitHub') {
      steps {
        git branch: 'master', url: 'https://github.com/yogi1754/pythonProject7.git'
        bat 'cp -r ./output/* ./new-project/'
        bat 'cd ./new-project && git add . && git commit -m "Update results" && git push'
      }
    }

    stage('Publish Results') {
      steps {
        publishHTML([allowMissing: false, alwaysLinkToLastBuild: true, keepAll: true, reportDir: 'figures', reportFiles: 'log_rating_over_time.png, violinplot_star_ratings.png'])
      }
    }
  }
     
