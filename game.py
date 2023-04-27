import urllib.request
import gzip
import csv
import json
import pyodbc as hr
import pymongo as pymongo
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Connect to MongoDB
client = pymongo.MongoClient(f"mongodb+srv://donyogeshwar:Welcome123@cluster0.xlzwbqj.mongodb.net/test")
database_name = 'amazon_reviews'
collection_name = 'gift_cards'
db = client[database_name]
collection = db[collection_name]

db.gift_cards.drop()

# Download and extract the dataset
url = 'https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Gift_Card_v1_00.tsv.gz'
filename = 'amazon_reviews_us_Gift_Card_v1_00.tsv.gz'
urllib.request.urlretrieve(url, filename)
with gzip.open(filename, 'rt', encoding='utf-8') as f:
    reader = csv.DictReader(f, delimiter='\t')
    for i, row in enumerate(reader):
        collection.insert_one(json.loads(json.dumps(row)))
        if i == 501:
            break

# Load data from MongoDB into a Pandas DataFrame
df = pd.DataFrame(list(collection.find({}, {'_id': 0, 'vine': 0, 'product_parent': 0, 'helpful_votes': 0, 'total_votes': 0})))

# Clean and normalize data
df['review_date'] = pd.to_datetime(df['review_date'])
df['star_rating'] = pd.to_numeric(df['star_rating'], errors='coerce')
df = df.dropna()

# Perform data transformation
df['log_rating'] = np.log(df['star_rating'])

# Handle outliers
q1, q3 = np.percentile(df['log_rating'], [25, 75])
iqr = q3 - q1
lower_bound = q1 - (1.5 * iqr)
upper_bound = q3 + (1.5 * iqr)
df = df[(df['log_rating'] >= lower_bound) & (df['log_rating'] <= upper_bound)]


# Connect to SQL Server database
server_name = '192.168.0.52,1433'
database_name = 'master'
username = 'methmi'
password = 'Welcome123#'

cnxn = hr.connect('DRIVER={SQL Server};SERVER='+server_name+';DATABASE='+database_name+';UID='+username+';PWD='+ password)

cursor = cnxn.cursor()

cursor.execute('DROP TABLE gift_card_reviews')

# Create gift_card_reviews table
cursor.execute('CREATE TABLE gift_card_reviews (marketplace varchar(255), customer_id varchar(255), review_id varchar(255), product_id varchar(255), product_title varchar(255), product_category varchar(255), star_rating int, verified_purchase varchar(255), review_headline varchar(255), review_body varchar(max), review_date date, log_rating float)')

# Insert data into SQL table
for index, row in df.iterrows():
 cursor.execute('INSERT INTO gift_card_reviews (marketplace, customer_id, review_id, product_id, product_title, product_category, star_rating, verified_purchase, review_headline, review_body, review_date, log_rating) values(?,?,?,?,?,?,?,?,?,?,?,?)',
  row['marketplace'], row['customer_id'], row['review_id'], row['product_id'], row['product_title'], row['product_category'], row['star_rating'], row['verified_purchase'], row['review_headline'], row['review_body'], row['review_date'], row['log_rating'])
cnxn.commit()

# Load data from SQL table into a Pandas DataFrame
sql_query = 'SELECT * FROM gift_card_reviews'
df = pd.read_sql(sql_query, cnxn)

# Data visualization using Seaborn
sns.set(style="ticks")
sns.set_palette("pastel")
sns.histplot(df, x="log_rating", hue="verified_purchase", kde=True, stat="density")
plt.title('Distribution of Logarithmic Star Ratings by Verified Purchase')
plt.xlabel('Logarithmic Star Rating')
plt.ylabel('Density')
plt.savefig('C:\\Users\\donyo\\OneDrive\\Documents\\images/Distribution of Logarithmic Star Ratings by Verified Purchase.png')
plt.close()

sns.boxplot(x="verified_purchase", y="log_rating", data=df)
plt.title('Boxplot of Logarithmic Star Ratings by Verified Purchase')
plt.xlabel('Verified Purchase')
plt.ylabel('Logarithmic Star Rating')
plt.savefig('C:\\Users\\donyo\\OneDrive\\Documents\\images/Boxplot of Logarithmic Star Ratings by Verified Purchase.png')
plt.close()

sns.boxplot(x='star_rating', data=df)
plt.xlabel('Star Rating')
plt.title('Distribution of Star Ratings')
plt.savefig('C:\\Users\\donyo\\OneDrive\\Documents\\images/Distribution of Star Ratings.png')
plt.close()

sns.countplot(x='verified_purchase', data=df)
plt.xlabel('Verified Purchase')
plt.ylabel('Count')
plt.title('Counts of Verified vs. Unverified Purchases')
plt.savefig('C:\\Users\\donyo\\OneDrive\\Documents\\images/Counts of Verified vs. Unverified Purchases.png')
plt.close()

# Visualize the data using a scatter plot
plt.scatter(df['review_date'], df['log_rating'])
plt.title("Logarithm of Rating Over Time")
plt.xlabel("review_date")
plt.ylabel("Logarithm of Rating")
plt.savefig('C:\\Users\\donyo\\OneDrive\\Documents\\images/logarithm_of_rating_over_time.png')
plt.close()

# Create a histogram of star ratings
plt.hist(df['star_rating'], bins=5)
plt.title("Histogram of Star Ratings")
plt.xlabel("Star Ratings")
plt.ylabel("Count")
plt.savefig('C:\\Users\\donyo\\OneDrive\\Documents\\images/histogram_of_star_ratings.png')
plt.close()

# Create a bar chart of the count of reviews by gift card category
category_counts = df.groupby('product_category')['review_id'].count()
plt.bar(category_counts.index, category_counts.values)
plt.title("Number of Reviews by Gift Card Category")
plt.xlabel("Gift Card Category")
plt.ylabel("Number of Reviews")
plt.xticks(rotation=90)
plt.savefig('C:\\Users\\donyo\\OneDrive\\Documents\\images/number_of_reviews_by_gift_card_category.png')
plt.close()

# Create a scatter plot matrix of the numerical variables in the dataset
sns.pairplot(df.select_dtypes(include=[np.number]))
plt.suptitle("Scatter Plot Matrix")
plt.savefig('C:\\Users\\donyo\\OneDrive\\Documents\\images/scatter_plot_matrix.png')
plt.close()

# Create a time series plot of the average star rating per month
df['year_month'] = df['review_date'].dt.to_period('M')
monthly_avg_rating = df.groupby('year_month')['star_rating'].mean()
plt.plot(monthly_avg_rating.index.to_timestamp(), monthly_avg_rating.values)
plt.title("Average Star Rating per Month")
plt.xlabel("Month")
plt.ylabel("Average Star Rating")
plt.savefig('C:\\Users\\donyo\\OneDrive\\Documents\\images/average_star_rating_per_month.png')
plt.close()

# Close the MongoDB connection
client.close()
