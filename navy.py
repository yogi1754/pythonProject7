import csv
import gzip
import json
import urllib.request
import pandas as pd
import pyodbc as pyodbc
import visualize
from pymongo import MongoClient
import re
import os
import string
import matplotlib.pyplot as plt
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import sqlalchemy
from sqlalchemy.dialects.mssql.information_schema import columns
from textblob import TextBlob
import os
import matplotlib.pyplot as plt

# from wordcloud import WordCloud

nltk.download('stopwords')
nltk.download('punkt')

# Connect to MongoDB
collection_name = 'review_watches'
uri = f"mongodb+srv://donyogeshwar:Welcome123@cluster0.xlzwbqj.mongodb.net/test"
client = MongoClient(uri)
database_name = 'amazon_reviews'
db = client[database_name]
collection = db[collection_name]

db.review_watches.drop()

# Download and extract the dataset
url = 'https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Watches_v1_00.tsv.gz'
filename = 'amazon_reviews_us_Watches_v1_00.tsv.gz'
urllib.request.urlretrieve(url, filename)
with gzip.open(filename, 'rt', encoding='utf-8') as f:
    reader = csv.DictReader(f, delimiter='\t')
    for i, row in enumerate(reader):
        collection.insert_one(json.loads(json.dumps(row)))
        if i == 101:
            break

# Connect to mongodb and get data
uri = f"mongodb+srv://donyogeshwar:Welcome123@cluster0.xlzwbqj.mongodb.net/test"
myclient = MongoClient(uri)
mydb = myclient["amazon_reviews12"]
mycol = mydb["review_watches"]

# Filter the necessary columns
myquery = {"product_category": "Watches"}
myprojection = {"_id", "review_id", "star_rating", "helpful_votes", "total_votes", "vine", "verified_purchase",
                "review_headline", "review_body", "review_date"}
mydoc = mycol.find(myquery, myprojection)

# Convert to pandas DataFrame
df = pd.DataFrame(list(mydoc))

# Clean the data
def clean_data(df):
    # Drop any rows with missing values
    df.dropna(inplace=True)

    # Remove any duplicate rows
    df.drop_duplicates(inplace=True)

    # Convert the review_date column to a datetime object
    df['review_date'] = pd.to_datetime(df['review_date'])

    # Remove any rows where the verified_purchase column is not 'Y' or 'N'
    df = df[df['verified_purchase'].isin(['Y', 'N'])]

    # Remove any rows where the review_body or review_title columns are empty strings
    df = df[df['review_body'] != '']

    return df

def clean_text_df(df, columns, remove_stopwords=True):
    """
    Clean text columns in a dataframe

    Parameters:
    df (pandas dataframe): the dataframe containing the text columns to be cleaned
    columns (list of str): the name(s) of the text column(s) to be cleaned
    remove_stopwords (bool): whether to remove stopwords or not (default True)

    Returns:
    df_clean (pandas dataframe): a new dataframe with cleaned text columns
    """
    # Create a copy of the original dataframe
    df_clean = df.copy()

    # Define function to clean a single text column
    stop_words = stopwords.words('english')

    def clean_text(text, remove_stopwords=True):
        # Convert to lowercase
        text = text.lower()
        # Remove URLs
        text = re.sub(r'http\S+', '', text)
        # Remove HTML tags
        text = re.sub(r'<.*?>', '', text)
        # Remove punctuation
        text = text.translate(str.maketrans('', '', string.punctuation))
        tokens = word_tokenize(text)

        # Remove stopwords
        if remove_stopwords:
            tokens = [word for word in tokens if not word in stop_words]

        text = ' '.join(tokens)
        return text

    # Create new dataframe with cleaned text columns
    for col in columns:
        df[col] = df[col].apply(clean_text)

    return df


df_clean = clean_data(df)

# Clean 'review_body' and 'review_headline'
text_columns = ['review_headline', 'review_body']
df_clean = clean_text_df(df_clean, text_columns)

df_clean.to_csv('cleaned_data.csv', index=False)

# Define connection string
server = '192.168.0.52,1433'
database = 'master'
username = 'methmi'
password = 'Welcome123#'
driver = '{ODBC Driver 17 for SQL Server}'
cnxn = pyodbc.connect(
    'DRIVER=' + driver + ';SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)

# Create table
cursor = cnxn.cursor()

cursor.execute('DROP TABLE review_watches')

cursor.execute("""
CREATE TABLE review_watches (_id VARCHAR(255),
review_id VARCHAR(255),
star_rating FLOAT,
helpful_votes INT,
total_votes INT,
vine VARCHAR(255),
verified_purchase VARCHAR(255),
review_headline TEXT,
review_body TEXT,
review_date DATETIME
)
""")

for index, row in df_clean.iterrows():
    review_id = row['review_id']
    star_rating = row['star_rating']
    helpful_votes = row['helpful_votes']
    total_votes = row['total_votes']
    vine = row['vine']
    verified_purchase = row['verified_purchase']
    review_headline = row['review_headline']
    review_body = row['review_body']
    review_date = row['review_date']

# Insert values into the SQL database
cursor.execute(f'''
INSERT INTO review_watches (review_id, star_rating, helpful_votes, total_votes, vine, verified_purchase, review_headline, review_body, review_date)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
''', review_id, star_rating, helpful_votes, total_votes, vine, verified_purchase, review_headline,
               review_body, review_date)

cnxn.commit()

# Extract data from Azure datastudio for visualization

cnxn = pyodbc.connect(
    'DRIVER=' + driver + ';SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)

# Query to select data from table
query = "SELECT * FROM review_watches"

# Use pandas read_sql function to read data into dataframe
reviews_all = pd.read_sql(query, cnxn)

# Close connection
cnxn.close()

# Define a dictionary mapping ratings to sentiments
sentiments_dict = {5: "Positive", 4: "Positive", 3: "Neutral", 2: "Negative", 1: "Negative"}

# Map the star_rating column to sentiments using the dictionary
reviews_all["sentiment_category"] = reviews_all["star_rating"].map(sentiments_dict)

save_dir = "C:\\Users\\donyo\\OneDrive\\Documents\\images\\navy\\"

# Get the mean rating of the product
mean_rating = reviews_all['star_rating'].mean()

# Plot the histogram of the stars / ratings given by the customers
plt.hist(reviews_all['star_rating'], bins=range(7))
plt.xlabel('Star Rating')
plt.ylabel('Count')
plt.title('Histogram of Star Ratings')
plt.savefig(os.path.join(save_dir, "hist_star_ratings.png"))

# Create a bar chart for verified purchase and star_rating
counts_verified_purchase = reviews_all.groupby(['star_rating', 'verified_purchase']).count()['review_id'].unstack()
ax = counts_verified_purchase.plot.bar()
ax.set_xlabel('Star rating')
ax.set_ylabel('Count')
ax.legend(title='Verified purchase', loc='upper left')
plt.title('Count of Verified Purchases by Star Rating')
plt.savefig(os.path.join(save_dir, 'bar_verified_purchases.png'))

# # replace missing values with an empty string
# visualize['review_body'] = visualize['review_body'].fillna('')
#
# # apply the len() function using a lambda function
# visualize['review_length'] = visualize['review_body'].apply(lambda x: len(x))
#
# # group by star_rating and calculate the mean review length
# df_grouped = visualize.groupby('star_rating')['review_length'].mean()
#
# # # Plot the bar chart
# plt.figure(figsize=(10, 8))
# plt.bar(df_grouped.index, df_grouped.values)
# plt.title('Average Review Length by Star Rating')
# plt.xlabel('Star Rating')
# plt.ylabel('Average Review Length')
# plt.savefig(os.path.join(save_dir, 'bar_review_length.png'))


# Plot frequent words for all review
class WordCloud:
    pass


# cloud = WordCloud(background_color='gray', max_font_size=60,
#                   relative_scaling=1).generate(' '.join(reviews_all.review_body))
# plt.figure(figsize=(10, 8))
# plt.imshow(cloud, interpolation='bilinear')
# plt.axis('off')
# plt.title(f'Wordcloud for all Star Ratings')
# plt.savefig(os.path.join(save_dir, 'wordcloud.png'))

# # Plot sentiment category
# cnt_sentiment = visualize['sentiment_category'].value_counts()

# # Plot a pie chart of the sentiment categories
# plt.figure(figsize=(10, 8))
# labels = ['Positive', 'Neutral', 'Negative']
# sizes = [cnt_sentiment['Positive'], cnt_sentiment['Neutral'], cnt_sentiment['Negative']]
# colors = ['yellowgreen', 'gold', 'lightskyblue']
# plt.pie(sizes, labels=labels, colors=colors,
#         autopct='%1.1f%%', startangle=140)
# plt.axis('equal')
# plt.title('Distribution of Sentiment Categories')
# plt.savefig(os.path.join(save_dir, 'sentiment_pie.png'))

client.close()
