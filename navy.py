import csv
import gzip
import json
import urllib.request
import pyodbc as pyodbc
import pandas as pd
import matplotlib.pyplot as plt
import pyodbc as pyodbc
from pymongo import MongoClient
import re
import wordcloud
from wordcloud import WordCloud
import re
import os
import string
import nltk
from nltk.corpus import stopwords
import pymssql
from textblob import TextBlob
nltk.download('stopwords')

# Connect to MongoDB
collection_name = 'review_watches'
uri = f"mongodb+srv://donyogeshwar:Welcome123@cluster0.xlzwbqj.mongodb.net/test"
# uri = 'mongodb://localhost:27017/'
client = MongoClient(uri)
database_name = 'amazon_reviews12'
db = client[database_name]
collection = db[collection_name]

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
# myclient = pymongo.MongoClient("mongodb://localhost:27017/")
uri = f"mongodb+srv://donyogeshwar:Welcome123@cluster0.xlzwbqj.mongodb.net/test"
# uri = 'mongodb://localhost:27017/'
myclient = MongoClient(uri)
mydb = myclient["amazon_reviews12"]
mycol = mydb["review_watches"]

# Filter the necessary columns
myquery = {"product_category": "Watches"}
myprojection = {"_id","review_id", "star_rating", "helpful_votes", "total_votes","vine","verified_purchase", "review_headline", "review_body","review_date"}
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
    df = df[(df['review_body'] != '') & (df['review_title'] != '')]

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

    # Define function to clean a single text column
    def clean_text(text):
        # Convert to lowercase
        text = text.lower()
        # Remove URLs
        text = re.sub(r'http\S+', '', text)
        # Remove HTML tags
        text = re.sub(r'<.*?>', '', text)
        # Remove punctuation
        text = text.translate(str.maketrans('', '', string.punctuation))
        # Remove digits
        text = re.sub(r'\d+', '', text)
        # Remove stopwords
        if remove_stopwords:
            stop_words = set(stopwords.words('english'))
            text = ' '.join([word for word in text.split() if word not in stop_words])
        return text

    # Create new dataframe with cleaned text columns
    df_clean = df.copy()
    for col in columns:
        df_clean[col] = df_clean[col].apply(clean_text)

    return df_clean


# Clean 'review_body' and 'review_headline'
data = clean_text_df(df, ['review_body', 'review_headline'])

# Define connection string
server = '192.168.0.52,1433'
database = 'master'
username = 'methmi'
password = 'Welcome123#'
driver= '{ODBC Driver 17 for SQL Server}'
cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)



# Create table
cursor = cnxn.cursor()

cursor.execute('DROP TABLE review_watches')

cursor.execute("""
    CREATE TABLE review_watches (_id VARCHAR(255),
    review_id VARCHAR(255),
    star_rating INT,
    helpful_votes INT,
    total_votes INT,
    vine VARCHAR(255),
    verified_purchase VARCHAR(255),
    review_headline VARCHAR(255),
    review_body VARCHAR(MAX),
    review_date DATETIME
    )
""")

# Convert DataFrame to list of tuples
#values = [tuple(x) for x in data.values]

insert_query = """INSERT INTO review_watches (_id, review_id, star_rating, helpful_votes, total_votes, vine, verified_purchase, review_headline, review_body, review_date)
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
cursor.execute(insert_query, [tuple(x) for x in data.values])


cnxn.commit()

# Close connection
cnxn.close()

# Extract data from Azure datastudio for visualization

cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

# Query to select data from table
query = "SELECT * FROM review_watches"

# Use pandas read_sql function to read data into dataframe
visualize = pd.read_sql(query, cnxn)

# Close connection
cnxn.close()

def sentiments(rating):
    if (rating == '5') or (rating == '4'):
        return "Positive"
    elif rating == '3':
        return "Neutral"
    elif (rating == '2') or (rating == '1'):
        return "Negative"


def plot_reviews(reviews_all, save_plots, save_dir=None):
    # Define save directory
    if save_plots and save_dir is None:
        save_dir = "./navy/"
        os.makedirs(save_dir, exist_ok=True)
    elif save_plots and save_dir is not None:
        os.makedirs(save_dir, exist_ok=True)
    elif not save_plots:
        save_dir = None

    # Create new column with value ['Positive', 'Neutral', 'Negative']
    visualize["sentiment_category"] = visualize["star_rating"].apply(sentiments)
    # Get the sentiment score for each review
    reviews_all['sentiment'] = reviews_all['review_body'].apply(lambda x: TextBlob(x).sentiment.polarity)
    # Get the mean rating of the product
    mean_rating = reviews_all['star_rating'].mean()

    # Plot the histogram of the stars / ratings given by the customers
    plt.hist(reviews_all['star_rating'], bins=range(7))
    plt.xlabel('Star Rating')
    plt.ylabel('Count')
    plt.title('Histogram of Star Ratings')

    if save_plots:
        plt.savefig('hist_star_ratings.png')
    plt.show()

    # Create a bar chart for verified purchase and star_rating
    counts_verified_purchase = reviews_all.groupby(['star_rating', 'verified_purchase']).count()['review_id'].unstack()

    ax = counts_verified_purchase.plot.bar()
    ax.set_xlabel('Star rating')
    ax.set_ylabel('Count')
    ax.legend(title='Verified purchase', loc='upper left')
    plt.title('Count of Verified Purchases by Star Rating')
    if save_plots:
        plt.savefig('bar_verified_purchases.png')
    plt.show()
    # Create a bar chart for text review length and star_rating
    visualize['review_length'] = visualize['review_body'].apply(len)
    df_grouped = visualize.groupby('star_rating').mean()['review_length']
    # Plot the bar chart
    plt.figure(figsize=(10, 8))
    plt.bar(df_grouped.index, df_grouped.values)
    plt.title('Average Review Length by Star Rating')
    plt.xlabel('Star Rating')
    plt.ylabel('Average Review Length')

    if save_plots:
        plt.savefig('bar_review_length.png')
    plt.show()
    # Plot frequent words for all review
    cloud = WordCloud(background_color='gray', max_font_size=60,
                      relative_scaling=1).generate(' '.join(reviews_all.review_body))
    plt.figure(figsize=(10, 8))
    plt.imshow(cloud, interpolation='bilinear')
    plt.axis('off')
    plt.title(f'Wordcloud for all Star Ratings')

    if save_plots:
        plt.savefig('wordcloud.png')
    plt.show()

    # Plot sentiment category
    cnt_sentiment = visualize['sentiment_category'].value_counts()

    # Plot a pie chart of the sentiment categories
    plt.figure(figsize=(10, 8))
    labels = ['Positive', 'Neutral', 'Negative']
    sizes = [cnt_sentiment['Positive'], cnt_sentiment['Neutral'], cnt_sentiment['Negative']]
    colors = ['yellowgreen', 'gold', 'lightskyblue']
    plt.pie(sizes, labels=labels, colors=colors,
            autopct='%1.1f%%', startangle=140)
    plt.axis('equal')
    plt.title('Distribution of Sentiment Categories')

    if save_dir is not None:
        # Save each plot as an image file
        plt.savefig(os.path.join(save_dir, "histogram.png"))
        plt.savefig(os.path.join(save_dir, "verified_purchase.png"))
        plt.savefig(os.path.join(save_dir, "avg_review_length.png"))
        plt.savefig(os.path.join(save_dir, "wordcloud.png"))
        plt.savefig(os.path.join(save_dir, "sentiment_pie.png"))
        plt.savefig(os.path.join(save_dir, "review_length_distribution.png"))

plot_reviews(visualize, "C:\\Users\\donyo\\OneDrive\\Documents\\images\\navy/")


client.close()
