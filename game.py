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
        if i == 20:
            break

# Load data from MongoDB into a Pandas DataFrame
gama = pd.DataFrame(
    list(collection.find({}, {'_id': 0, 'vine': 0, 'product_parent': 0, 'helpful_votes': 0, 'total_votes': 0})))

# Clean and normalize data
gama['review_date'] = pd.to_datetime(gama['review_date'])
gama['star_rating'] = pd.to_numeric(gama['star_rating'], errors='coerce')
gama = gama.dropna()

# Perform data transformation
gama['log_rating'] = np.log(gama['star_rating'])

# Handle outliers
q1, q3 = np.percentile(gama['log_rating'], [25, 75])
iqr = q3 - q1
lower_bound = q1 - (1.5 * iqr)
upper_bound = q3 + (1.5 * iqr)
df = gama[(gama['log_rating'] >= lower_bound) & (gama['log_rating'] <= upper_bound)]

# Connect to SQL Server database
server_name = '192.168.0.52,1433'
database_name = 'master'
username = 'methmi'
password = 'Welcome123#'

cnxn = hr.connect(
    'DRIVER={SQL Server};SERVER=' + server_name + ';DATABASE=' + database_name + ';UID=' + username + ';PWD=' + password)

cursor = cnxn.cursor()

cursor.execute('DROP TABLE gift_card_reviews')

# Create gift_card_reviews table
cursor.execute(
    'CREATE TABLE gift_card_reviews (marketplace varchar(255), customer_id varchar(255), review_id varchar(255), product_id varchar(255), product_title varchar(255), product_category varchar(255), star_rating int, verified_purchase varchar(255), review_headline varchar(255), review_body varchar(max), review_date date, log_rating float)')

# Insert data into SQL table
for index, row in df.iterrows():
    cursor.execute(
        'INSERT INTO gift_card_reviews (marketplace, customer_id, review_id, product_id, product_title, product_category, star_rating, verified_purchase, review_headline, review_body, review_date, log_rating) values(?,?,?,?,?,?,?,?,?,?,?,?)',
        row['marketplace'], row['customer_id'], row['review_id'], row['product_id'], row['product_title'],
        row['product_category'], row['star_rating'], row['verified_purchase'], row['review_headline'],
        row['review_body'], row['review_date'], row['log_rating'])
cnxn.commit()

# Load data from SQL table into a Pandas DataFrame
sql_query = 'SELECT * FROM gift_card_reviews'
gama = pd.read_sql(sql_query, cnxn, parse_dates=['review_date'])

sns.countplot(x='verified_purchase', data=gama)
plt.xlabel('Verified Purchase')
plt.ylabel('Count')
plt.title('Counts of Verified vs. Unverified Purchases')
plt.savefig('C:\\Users\\donyo\\OneDrive\\Documents\\images\\yogesh/Counts of Verified vs. Unverified Purchases.png')
plt.close()

# Create a histogram of star ratings
plt.hist(gama['star_rating'], bins=5)
plt.title("Histogram of Star Ratings")
plt.xlabel("Star Ratings")
plt.ylabel("Count")
plt.savefig('C:\\Users\\donyo\\OneDrive\\Documents\\images\\yogesh/histogram_of_star_ratings.png')
plt.close()

# Create a bar chart of the count of reviews by gift card category
category_counts = gama.groupby('product_category')['review_id'].count()
plt.bar(category_counts.index, category_counts.values)
plt.title("Number of Reviews by Gift Card Category")
plt.xlabel("Gift Card Category")
plt.ylabel("Number of Reviews")
plt.xticks(rotation=90)
plt.savefig('C:\\Users\\donyo\\OneDrive\\Documents\\images\\yogesh/number_of_reviews_by_gift_card_category.png')
plt.close()

# Create a scatter plot matrix of the numerical variables in the dataset
sns.pairplot(gama.select_dtypes(include=[np.number]))
plt.suptitle("Scatter Plot Matrix")
plt.savefig('C:\\Users\\donyo\\OneDrive\\Documents\\images\\yogesh/scatter_plot_matrix.png')
plt.close()

# Close the MongoDB connection
client.close()
