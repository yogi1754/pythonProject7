pipeline {
    agent any
    
    stages {
        stage('Install Packages') {
            steps {
                bat 'Py -m pip install pymongo' 
		bat 'Py -m pip install pandas'
		bat 'Py -m pip install numpy' 
		bat 'Py -m pip install scikit-learn' 
		bat 'Py -m pip install matplotlib' 
		bat 'Py -m pip install seaborn'
            }
        }
        
  stage('Download and Extract Dataset') {
            steps {
                bat '''
                    @echo off
                    curl -O https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Gift_Card_v1_00.tsv.gz 
                    gzip -d amazon_reviews_us_Gift_Card_v1_00.tsv.gz 
                    more +1010 amazon_reviews_us_Gift_Card_v1_00.tsv > amazon_reviews_us_Gift_Card_v1_00_limit_1010.tsv 
                '''
            }
        }

      stage('Data Processing') {
  steps {
    // Import required modules and libraries
    bat '''
    Py -m pip install pymongo pandas numpy scikit-learn matplotlib seaborn org.bson.Document com.mongodb.client.model.Filters.* com.mongodb.client.MongoClients com.mongodb.client.MongoCollection com.mongodb.client.MongoClient com.mongodb.client.MongoDatabase com.mongodb.client.model.Filters com.mongodb.client.model.Updates com.mongodb.client.result.DeleteResult com.mongodb.client.result.UpdateResult com.mongodb.BasicDBObject com.mongodb.DBObject com.mongodb.util.JSON com.mongodb.MongoCredential com.mongodb.MongoClientURI com.mongodb.MongoClientOptions
    '''
      
 // Connect to MongoDB
client = pymongo.MongoClient()
database_name = 'amazon_reviews'
collection_name = 'gift_cards'
db = client[database_name]
collection = db[collection_name]

// Download and extract the dataset
url = 'https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Gift_Card_v1_00.tsv.gz'
filename = 'amazon_reviews_us_Gift_Card_v1_00.tsv.gz'
urllib.request.urlretrieve(url, filename)
with gzip.open(filename, 'rt', encoding='utf-8') as f=
    reader = csv.DictReader(f, delimiter='\t')
    reader.eachWithIndex { row, i ->
    collection.insertOne(json.loads(json.dumps(row)))
    if (i == 1010) {
        return
    }
}
     

// Load data from MongoDB into a Pandas DataFrame
df = pd.DataFrame(list(collection.find()))


      
    // Clean and normalize data
    df = df.drop("_id")
    df['review_date'] = pd.to_datetime(df['review_date'])
    df['star_rating'] = pd.to_numeric(df['star_rating'], errors='coerce')
    df = df.dropna()
      
    // Perform data transformation
    df['log_rating'] = np.log(df['star_rating'])
      
    // Handle outliers
    def q1, q3 = np.percentile(df['log_rating'], [25, 75])
    def iqr = q3 - q1
    def lower_bound = q1 - (1.5 * iqr)
    def upper_bound = q3 + (1.5 * iqr)
    df = df[(df['log_rating'] >= lower_bound) & (df['log_rating'] <= upper_bound)]
    
    // Connect to SQL Server
    def cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=YOGESH\\SQLEXPRESS;DATABASE=database_name')
   def db_cursor = cnxn.cursor()
    
    // Create SQL table for data
    cursor.execute('CREATE TABLE gift_card_reviews (marketplace varchar(255), customer_id varchar(255), review_id varchar(255), product_id varchar(255), product_parent varchar(255), product_title varchar(255), product_category varchar(255), star_rating int, helpful_votes int, total_votes int, vine varchar(255), verified_purchase varchar(255), review_headline varchar(255), review_body varchar(max), review_date date, log_rating float)')
    
    // Insert data into SQL table
    for (def row : df.iterrows()) {
      cursor.execute('INSERT INTO gift_card_reviews (marketplace, customer_id, review_id, product_id, product_parent, product_title, product_category, star_rating, helpful_votes, total_votes, vine, verified_purchase, review_headline, review_body, review_date, log_rating) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                  row['marketplace'], row['customer_id'], row['review_id'], row['product_id'], row['product_parent'], row['product_title'], row['product_category'], row['star_rating'], row['helpful_votes'], row['total_votes'], row['vine'], row['verified_purchase'], row['review_headline'], row['review_body'], row['review_date'], row['log_rating'])
    }
    cnxn.commit()


		    // Close the SQL connection
		    cursor.close()
		    cnxn.close()

                    // Visualize the data using a scatter plot
                    plt.scatter(df['review_date'], df['log_rating'])
                    plt.title("Logarithm of Rating Over Time")
                    plt.xlabel("review_date")
                    plt.ylabel("Logarithm of Rating")
                    plt.savefig('https://github.com/yogi1754/pythonProject7.git/Logarithm of Rating Over Time.png')
		    plt.close()
                    
		    // Create a bar chart of the count of reviews by gift card category
                    category_counts = df.groupby('product_category')['review_id'].count()
                    plt.bar(category_counts.index, category_counts.values)
		    plt.title("Number of Reviews by Gift Card Category")
		    plt.xlabel("Gift Card Category")
		    plt.ylabel("Number of Reviews")
		    plt.xticks(rotation=90)
		    plt.savefig('https://github.com/yogi1754/pythonProject7.git/number_of_reviews_by_gift_card_category.png')
		    plt.close()

					// Create a scatter plot matrix of the numerical variables in the dataset
					sns.pairplot(df.select_dtypes(include=[np.number]))
					plt.suptitle("Scatter Plot Matrix")
					plt.savefig('https://github.com/yogi1754/pythonProject7.git/scatter_plot_matrix.png')
					plt.close()

					// Create a time series plot of the average star rating per month
					df['year_month'] = df['review_date'].dt.to_period('M')
					monthly_avg_rating = df.groupby('year_month')['star_rating'].mean()
					plt.plot(monthly_avg_rating.index.to_timestamp(), monthly_avg_rating.values)
					plt.title("Average Star Rating per Month")
					plt.xlabel("Month")
					plt.ylabel("Average Star Rating")
					plt.savefig('https://github.com/yogi1754/pythonProject7.git/average_star_rating_per_month.png')
					plt.close()

					// Fit a linear regression model
					X = df[['review_date']].astype(int)
					y = df['star_rating']
					model = LinearRegression().fit(X, y)

                    // Create a histogram of star ratings
                    plt.hist(df['star_rating'], bins=5)
                    plt.title("Histogram of Star Ratings")
                    plt.xlabel("Star Ratings")
                    plt.ylabel("Count")
                    plt.savefig('https://github.com/yogi1754/pythonProject7.git/Histogram of Star Ratings.png')
		    plt.close()

                    // Create a bar chart of the count of reviews by gift card category
                    def category_counts = df.groupby('product_category')['review_id'].count()
                    plt.bar(category_counts.index, category_counts.values)
                    plt.title("Number of Reviews by Gift Card Category")
                    plt.xlabel("Gift Card Category")
                    plt.ylabel("Number of Reviews")
                    plt.xticks(rotation=90)
		    plt.savefig('https://github.com/yogi1754/pythonProject7.git/number_of_reviews_by_gift_card_category.png')
		    plt.close()


                    // Create a violinplot of the star ratings and save the figure
		    sns.violinplot(x='star_rating', data=df)
		    plt.title("Violinplot of Star Ratings")
	            plt.savefig('https://github.com/yogi1754/pythonProject7.git/violinplot_star_ratings.png')
      
      // Close the MongoDB connection
      client.close()
      
    
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
}
