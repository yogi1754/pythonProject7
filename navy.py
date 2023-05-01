import csv
import gzip
import json
import urllib.request
import pandas as pd
import pyodbc as pyodbc
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
from wordcloud import WordCloud

nltk.download('stopwords')
nltk.download('punkt')

# Connect to MongoDB
collection_name = 'review_watches'
uri = f"mongodb+srv://donyogeshwar:Welcome123@cluster0.xlzwbqj.mongodb.net/test"
client = MongoClient(uri)
database_name = 'amazon_reviews12'
db = client[database_name]
collection = db[collection_name]

#db.review_watches.drop()

# Download and extract the dataset
url = 'https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Watches_v1_00.tsv.gz'
filename = 'amazon_reviews_us_Watches_v1_00.tsv.gz'
urllib.request.urlretrieve(url, filename)
with gzip.open(filename, 'rt', encoding='utf-8') as f:
    reader = csv.DictReader(f, delimiter='\t')
    for i, row in enumerate(reader):
        collection.insert_one(json.loads(json.dumps(row)))
        if i == 1010:
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
def clean_data(df2):
    # Drop any rows with missing values
    df.dropna(inplace=True)

    # Remove any duplicate rows
    df.drop_duplicates(inplace=True)

    # Convert the review_date column to a datetime object
    df['review_date'] = pd.to_datetime(df['review_date'])

    # Remove any rows where the verified_purchase column is not 'Y' or 'N'
    df1 = df[df['verified_purchase'].isin(['Y', 'N'])]

    # Remove any rows where the review_body or review_title columns are empty strings
    df2 = df[df['review_body'] != '']

    return df2


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

    def clean_text(text, remove_stopwords=None):
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
        df_clean[col] = df_clean[col].apply(clean_text)

    return df_clean

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

#cursor.execute('DROP TABLE review_watches')

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
visualize = pd.read_sql(query, cnxn)

# Close connection
cnxn.close()


def sentiments(star_rating):
    if star_rating in [1, 2]:
        return 'Negative'
    elif star_rating == 3:
        return 'Neutral'
    elif star_rating in [4, 5]:
        return 'Positive'


class WordCloud:
    pass


def plot_reviews(reviews_all, save_plots, save_dir=None):
    # Define save directory
    if save_plots and save_dir is None:
        save_dir = "C:\\Users\\donyo\\OneDrive\\Documents\\images\\navy/"
        os.makedirs(save_dir, exist_ok=True)
    elif save_plots and save_dir is not None:
        os.makedirs(save_dir, exist_ok=True)
    elif not save_plots:
        save_dir = None

    # Create new column with value ['Positive', 'Neutral', 'Negative']
    reviews_all["sentiment_category"] = reviews_all["star_rating"].apply(sentiments)
    # Get the sentiment score for each review
    reviews_all['sentiment'] = reviews_all['review_body'].apply(lambda x: TextBlob(x).sentiment.polarity)
    # Get the mean rating of the product
    mean_rating = reviews_all['star_rating'].mean()

    # Plot the histogram of the stars / ratings given by the customers

    plt.hist(reviews_all['star_rating'], bins=range(7))
    plt.xlabel('Star Rating')
    plt.ylabel('Count')
    plt.title('Histogram of Star Ratings')
    if save_dir:
        plt.savefig(os.path.join(save_dir, 'hist_star_ratings.png'))
        plt.close()

        # Create a bar chart for verified purchase and star_rating

        counts_verified_purchase = reviews_all.groupby(['star_rating', 'verified_purchase']).count()[
            'review_id'].unstack()
        ax = counts_verified_purchase.plot.bar()
        ax.set_xlabel('Star rating')
        ax.set_ylabel('Count')
        ax.legend(title='Verified purchase', loc='upper left')
        plt.title('Count of Verified Purchases by Star Rating')
        save_dir = 'path/to/save/dir'  # define save_dir here
        if save_dir:
            plt.savefig(os.path.join(save_dir, 'bar_verified_purchases.png'))
            plt.close()

        # Plot frequent words for all review
        cloud = WordCloud(background_color='gray', max_font_size=60, relative_scaling=1).generate(
            ' '.join(reviews_all.review_body))
        plt.figure(figsize=(10, 8))
        plt.imshow(cloud, interpolation='bilinear')
        plt.axis('off')
        plt.title(f'Wordcloud for all Star Ratings')
        if save_dir:
            plt.savefig(os.path.join(save_dir, 'wordcloud.png'))
            plt.close()

        # Plot sentiment category
        cnt_sentiment = reviews_all['sentiment_category'].value_counts()

        # Group the data by star_rating column
        df_grouped = reviews_all.groupby('star_rating').count()

        # Plot a bar chart of the groupby result
        plt.bar(df_grouped.index, df_grouped['review_id'])
        plt.xlabel('Star rating')
        plt.ylabel('Count')
        plt.title('Count of sentiments by Star Rating')
        if save_dir:
            plt.savefig(os.path.join(save_dir, 'bar_sentiments.png'))
            plt.close()

        # Plot the sentiment score distribution
        plt.hist(reviews_all['sentiment'], bins=20)
        plt.xlabel('Sentiment Score')
        plt.ylabel('Count')
        plt.title('Histogram of Sentiment Scores')
        if save_dir:
            plt.savefig(os.path.join(save_dir, 'hist_sentiment_scores.png'))
            plt.close()

        # Plot the mean sentiment score for each star rating
        mean_sentiment_by_rating = reviews_all.groupby('star_rating')['sentiment'].mean()
        plt.plot(mean_sentiment_by_rating.index, mean_sentiment_by_rating, '-o')
        plt.xlabel('Star rating')
        plt.ylabel('Mean Sentiment Score')
        plt.title('Mean Sentiment Score by Star Rating')
        if save_dir:
            plt.savefig(os.path.join(save_dir, 'line_mean_sentiment_by_rating.png'))
            plt.close()

        # Plot the mean rating for each sentiment category
        mean_rating_by_sentiment = reviews_all.groupby('sentiment_category')['star_rating'].mean()
        plt.bar(mean_rating_by_sentiment.index, mean_rating_by_sentiment)
        plt.xlabel('Sentiment Category')
        plt.ylabel('Mean Rating')
        plt.title('Mean Rating by Sentiment Category')
        if save_dir:
            plt.savefig(os.path.join(save_dir, 'bar_mean_rating_by_sentiment.png'))
            plt.close()

        # Print summary statistics
        mean_rating = reviews_all['star_rating'].mean()
        print(f"Number of reviews: {len(reviews_all)}")
        print(f"Mean rating: {mean_rating:.2f}")
        print(f"Number of positive reviews: {cnt_sentiment['Positive']}")
        print(f"Number of neutral reviews: {cnt_sentiment['Neutral']}")
        print(f"Number of negative reviews: {cnt_sentiment['Negative']}")

        client.close()
