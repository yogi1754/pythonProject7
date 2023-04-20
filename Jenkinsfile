pipeline {
    agent any
    
    stages {
        stage('Install Packages') {
            steps {
                bat 'Py -m install pymongo' 
		bat 'Py install pandas'
		bat 'Py install numpy' 
		bat 'Py install scikit-learn' 
		bat 'Py install matplotlib' 
		bat 'Py install seaborn'
            }
        }
        
        stage('Download and Extract Dataset') {
            steps {
        bat 'curl -O https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Gift_Card_v1_00.tsv.gz'
        bat 'gzip -d amazon_reviews_us_Gift_Card_v1_00.tsv.gz'
        bat 'head -n 1010 amazon_reviews_us_Gift_Card_v1_00.tsv > amazon_reviews_us_Gift_Card_v1_00_limit_1010.tsv'
    }
        }
        
        stage('Data Processing') {
            steps {
                bat '''
                    python3 - <<EOF
                    import csv
                    import json
                    from pymongo import MongoClient
                    import pandas as pd
                    import numpy as np
                    from sklearn.linear_model import LinearRegression
                    import matplotlib.pyplot as plt
                    import seaborn as sns

                    # Connect to MongoDB
		    client = MongoClient()
                    database_name = 'amazon_reviews'
                    collection_name = 'gift_cards'
                    db = client[database_name]
                    collection = db[collection_name]


                    // Download and extract the dataset
                    def reader = new CSVReader(new FileReader("${filename}"), '\t')
                    def i = 0
                    def json_row
                    while ((json_row = reader.readNext()) != null && i < 1010) {
                        collection.insertOne(Document.parse(new JSONObject(json_row).toString()))
                        i++
                    }
                    
                    // Load data from MongoDB into a Pandas DataFrame
                    def cursor = collection.find()
                    List<Document> documents = new ArrayList<Document>()
                    cursor.into(documents)
                    def df = new DataFrame(documents)

                    // Clean and normalize data
                    df = df.drop("_id")
                    df['review_date'] = pd.to_datetime(df['review_date'])
                    df['star_rating'] = pd.to_numeric(df['star_rating'], errors='coerce')
                    df = df.dropna()

                    // Perform data transformation
                    df['log_rating'] = np.log(df['star_rating'])
		    
		    # Handle outliers
		    q1, q3 = np.percentile(df['log_rating'], [25, 75])
		    iqr = q3 - q1
		    lower_bound = q1 - (1.5 * iqr)
		    upper_bound = q3 + (1.5 * iqr)
		    df = df[(df['log_rating'] >= lower_bound) & (df['log_rating'] <= upper_bound)]

                    # Connect to SQL Server
		    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=YOGESH\\SQLEXPRESS;DATABASE=database_name')
		    cursor = cnxn.cursor()
  
		    # Create SQL table for data
		    cursor.execute('CREATE TABLE gift_card_reviews (review_date date, star_rating int, log_rating float)')
  
		    # Insert data into SQL table
		    for index, row in df.iterrows():
		    cursor.execute('INSERT INTO gift_card_reviews (review_date, star_rating, log_rating) values(?,?,?)', row['review_date'], row['star_rating'], row['log_rating'])
		    cnxn.commit()
  
		    # Close the SQL connection
		    cursor.close()
		    cnxn.close()

                    // Visualize the data using a scatter plot
                    plt.scatter(df['review_date'], df['log_rating'])
                    plt.title("Logarithm of Rating Over Time")
                    plt.xlabel("review_date")
                    plt.ylabel("Logarithm of Rating")
                    plt.savefig('https://github.com/yogi1754/pythonProject7.git/Logarithm of Rating Over Time.png')
		    plt.close()
                    
		    # Create a bar chart of the count of reviews by gift card category
                    category_counts = df.groupby('product_category')['review_id'].count()
                    plt.bar(category_counts.index, category_counts.values)
		    plt.title("Number of Reviews by Gift Card Category")
		    plt.xlabel("Gift Card Category")
		    plt.ylabel("Number of Reviews")
		    plt.xticks(rotation=90)
		    plt.savefig('https://github.com/yogi1754/pythonProject7.git/number_of_reviews_by_gift_card_category.png')
		    plt.close()

					# Create a scatter plot matrix of the numerical variables in the dataset
					sns.pairplot(df.select_dtypes(include=[np.number]))
					plt.suptitle("Scatter Plot Matrix")
					plt.savefig('https://github.com/yogi1754/pythonProject7.git/scatter_plot_matrix.png')
					plt.close()

					# Create a time series plot of the average star rating per month
					df['year_month'] = df['review_date'].dt.to_period('M')
					monthly_avg_rating = df.groupby('year_month')['star_rating'].mean()
					plt.plot(monthly_avg_rating.index.to_timestamp(), monthly_avg_rating.values)
					plt.title("Average Star Rating per Month")
					plt.xlabel("Month")
					plt.ylabel("Average Star Rating")
					plt.savefig('https://github.com/yogi1754/pythonProject7.git/average_star_rating_per_month.png')
					plt.close()

					# Fit a linear regression model
					X = df[['review_date']].astype(int)
					y = df['star_rating']
					model = LinearRegression().fit(X, y)

                    # Create a histogram of star ratings
                    plt.hist(df['star_rating'], bins=5)
                    plt.title("Histogram of Star Ratings")
                    plt.xlabel("Star Ratings")
                    plt.ylabel("Count")
                    plt.savefig('https://github.com/yogi1754/pythonProject7.git/Histogram of Star Ratings.png')
		    plt.close()

                    # Create a bar chart of the count of reviews by gift card category
                    def category_counts = df.groupby('product_category')['review_id'].count()
                    plt.bar(category_counts.index, category_counts.values)
                    plt.title("Number of Reviews by Gift Card Category")
                    plt.xlabel("Gift Card Category")
                    plt.ylabel("Number of Reviews")
                    plt.xticks(rotation=90)
		    plt.savefig('https://github.com/yogi1754/pythonProject7.git/number_of_reviews_by_gift_card_category.png')
		    plt.close()


                    # Create a violinplot of the star ratings and save the figure
		    sns.violinplot(x='star_rating', data=df)
		    plt.title("Violinplot of Star Ratings")
	            plt.savefig('https://github.com/yogi1754/pythonProject7.git/violinplot_star_ratings.png')
      
      # Close the MongoDB connection
      client.close()
      "
    '''
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
