import csv
import gzip
import json
import urllib.request
import pyodbc as db
import pandas as pd
import matplotlib.pyplot as plt
import pyodbc
from pymongo import MongoClient
from wordcloud import WordCloud
import sqlalchemy


# Connect to MongoDB
collection_name = 'us_software'
uri = f"mongodb+srv://donyogeshwar:Welcome123@cluster0.xlzwbqj.mongodb.net/test"
client = MongoClient(uri)
database_name = 'amazon_reviews'
db = client[database_name]
collection = db[collection_name]

db.us_software.drop()

# Download and extract the dataset
url = 'https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Software_v1_00.tsv.gz'
filename = 'amazon_reviews_us_Software_v1_00.tsv.gz'
urllib.request.urlretrieve(url, filename)
with gzip.open(filename, 'rt', encoding='utf-8') as f:
    reader = csv.DictReader(f, delimiter='\t')
    for i, row in enumerate(reader):
        collection.insert_one(json.loads(json.dumps(row)))
        if i == 1010:
            break

# Retrieve the documents from the collection, excluding "vine", "product_parent" columns
cursor = collection.find({}, {"vine": 0, "product_parent": 0})

# Convert the cursor to a list of dictionaries
documents = list(cursor)


# store them in a DataFrame
df = pd.DataFrame(documents)

df['_id'] = df['_id'].astype(str)
df['review_date'] = pd.to_datetime(df['review_date'])
df['star_rating'] = df['star_rating'].astype(int)
df['total_votes'] = df['total_votes'].astype(int)
df['helpful_votes'] = df['helpful_votes'].astype(int)
df['review_body'] = df['review_body'].str.slice(0, 1000)

# # Clean the data
df = df.drop_duplicates()
df = df.fillna(0)

# Get the data types of each column in the DataFrame
#print(df.dtypes)

server_name = '192.168.0.52,1433'
database_name = 'master'
username = 'methmi'
password = 'Welcome123#'

cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server_name+';DATABASE='+database_name+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()
cursor.execute('DROP TABLE amazon_sw')
#Increase the maximum allowed length of the review_body column to 2000 characters
cursor.execute('CREATE TABLE amazon_sw (_id VARCHAR(100), marketplace VARCHAR(50), customer_id VARCHAR(50), review_id VARCHAR(50), product_id VARCHAR(50), product_title VARCHAR(1000),'
               'product_category VARCHAR(150), star_rating INT, helpful_votes INT, total_votes INT,'
               'verified_purchase VARCHAR(50), review_headline VARCHAR(200), review_body VARCHAR(1000), review_date date)')

for index, row in df.iterrows():
    # Truncate the review_body to the maximum allowed length
    #max_length = 1000 # replace with the maximum allowed length for review_body column
    #review_body = row['review_body'][:max_length]
    review_body = row['review_body'][:1000] # truncate to 1000 characters
    cursor.execute("INSERT INTO amazon_sw (_id, marketplace, customer_id, review_id, product_id, product_title, product_category, star_rating, helpful_votes, total_votes, verified_purchase, review_headline, review_body, review_date)"
                   "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                   row['_id'], row['marketplace'], row['customer_id'], row['review_id'], row['product_id'],row['product_title'],
                   row['product_category'], row['star_rating'], row['helpful_votes'], row['total_votes'], row['verified_purchase'],
                   row['review_headline'], row['review_body'], row['review_date'])
    cnxn.commit()


# create a cursor object to execute SQL queries
cursor = cnxn.cursor()

# execute the SQL query
query = "SELECT * FROM amazon_sw"

# try:

df = pd.read_sql(query, cnxn)

# Select the column containing the star ratings
star_ratings = df['star_rating']

# Create a histogram of star ratings
plt.hist(star_ratings, bins=10, color='purple', alpha=0.8)

# Set the title and axis labels
plt.title('Distribution of Star Ratings')
plt.xlabel('Star Rating')
plt.ylabel('Count')

plt.savefig('C:\\Users\\donyo\\OneDrive\\Documents\\images\\methmi/histogram.png')
plt.close()

# plot scatter plot of star rating vs helpful votes
plt.scatter(df['star_rating'], df['helpful_votes'])
plt.title('Star Rating vs Helpful Votes')
plt.xlabel('Star Rating')
plt.ylabel('Helpful Votes')

plt.savefig('C:\\Users\\donyo\\OneDrive\\Documents\\images\\methmi/scatter_plot1.png')
plt.close()

# plot scatter plot of star rating vs total votes
plt.scatter(df['star_rating'], df['total_votes'])
plt.title('Star Rating vs Total Votes')
plt.xlabel('Star Rating')
plt.ylabel('Total Votes')

plt.savefig('C:\\Users\\donyo\\OneDrive\\Documents\\images\\methmi/scatter_plot.png')

plt.close()

df = pd.read_sql_query("SELECT verified_purchase FROM amazon_sw", cnxn)

# Count the number of verified and non-verified purchases
verified_counts = df['verified_purchase'].value_counts()

# Plot the bar graph
plt.bar(verified_counts.index, verified_counts.values)
plt.title('Verified vs Non-Verified Purchases')
plt.xlabel('Verified Purchase')
plt.ylabel('Number of Purchases')

plt.savefig('C:\\Users\\donyo\\OneDrive\\Documents\\images\\methmi/Bar_Graph.png')

plt.close()

df = pd.read_sql_query("SELECT review_headline FROM amazon_sw", cnxn)

# Combine all review headlines into a single string
all_reviews_headline = " ".join(review_headline for review_headline in df['review_headline'])

# Generate a word cloud
wordcloud = WordCloud(width=800, height=800, background_color='white').generate(all_reviews_headline)

# Plot the word cloud
plt.figure(figsize=(8, 8), facecolor=None)
plt.imshow(wordcloud)
plt.axis("off")
plt.tight_layout(pad=0)

plt.savefig('C:\\Users\\donyo\\OneDrive\\Documents\\images\\methmi/WordCloud1.png')
plt.close()

df = pd.read_sql_query("SELECT review_body FROM amazon_sw", cnxn)

# Combine all review bodies into a single string
all_reviews_body = " ".join(review_body for review_body in df['review_body'])

# Generate a word cloud
wordcloud = WordCloud(width=800, height=800, background_color='white').generate(all_reviews_body)

# Plot the word cloud
plt.figure(figsize=(8, 8), facecolor=None)
plt.imshow(wordcloud)
plt.axis("off")
plt.tight_layout(pad=0)

plt.savefig('C:\\Users\\donyo\\OneDrive\\Documents\\images\\methmi/WordCloud.png')
plt.close()

client.close()





