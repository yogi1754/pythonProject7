import csv
import gzip
import json
import urllib.request
import pyodbc as db
import pandas as pd
import matplotlib.pyplot as plt
import pyodbc as pyodbc
from pymongo import MongoClient
import re
import wordcloud
from wordcloud import WordCloud
import re
import string
import nltk
from nltk.corpus import stopwords
import pymssql
from textblob import TextBlob
nltk.download('stopwords')



# Download and extract the dataset
url = 'https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Watches_v1_00.tsv.gz'
filename = 'amazon_reviews_us_Watches_v1_00.tsv.gz'
urllib.request.urlretrieve(url, filename)

# Connect to MongoDB
collection_name = 'review_watches'
uri = f"mongodb+srv://donyogeshwar:Welcome123@cluster0.xlzwbqj.mongodb.net/test"
client = MongoClient(uri)
#db.review_watches.drop()
database_name = 'amazon_reviews12'
db = client[database_name]
collection = db[collection_name]

with gzip.open(filename, 'rt', encoding='utf-8') as f:
    reader = csv.DictReader(f, delimiter='\t')
    for i, row in enumerate(reader):
        collection.insert_one(json.loads(json.dumps(row)))
        if i == 1010:
            break


uri = f"mongodb+srv://donyogeshwar:Welcome123@cluster0.xlzwbqj.mongodb.net/test"
myclient = MongoClient(uri)
mydb = myclient["amazon_reviews12"]
mycol = mydb["review_watches"]

# Filter the necessary columns
myquery = {"product_category": "Watches"}
myprojection = {"review_id", "star_rating", "helpful_votes", "total_votes", "vine", "verified_purchase",
                "review_headline", "review_body", "review_date"}
mydoc = mycol.find(myquery, myprojection)

# Convert to pandas DataFrame
df = pd.DataFrame(list(mydoc))

# Print column names in the DataFrame
print(df.columns)

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
    df = df[(df['body'] != '') & (df['title'] != '')]

    # Remove any rows where the star_rating column is not a number between 1 and 5
    df = df[df['star_rating'].astype(float).isin([1.0, 2.0, 3.0, 4.0, 5.0])]

    return df

# Preprocessing
# Remove punctuations, stopwords, and lemmatize for review columns: 'review_body' and 'review_headline'
# A list of contractions from http://stackoverflow.com/questions/19790188/expanding-english-language-contractions-in-python
contractions = {
    "ain't": "am not",
    "aren't": "are not",
    "can't": "cannot",
    "can't've": "cannot have",
    "'cause": "because",
    "could've": "could have",
    "couldn't": "could not",
    "couldn't've": "could not have",
    "didn't": "did not",
    "doesn't": "does not",
    "don't": "do not",
    "hadn't": "had not",
    "hadn't've": "had not have",
    "hasn't": "has not",
    "haven't": "have not",
    "he'd": "he would",
    "he'd've": "he would have",
    "he'll": "he will",
    "he'll've": "he will have",
    "he's": "he is",
    "how'd": "how did",
    "how'd'y": "how do you",
    "how'll": "how will",
    "how's": "how is",
    "I'd": "I would",
    "I'd've": "I would have",
"i'll": "i will",
"i'm": "i am",
"i've": "i have",
"isn't": "is not",
"it'd": "it would",
"it'll": "it will",
"it's": "it is",
"let's": "let us",
"ma'am": "madam",
"mayn't": "may not",
"might've": "might have",
"mightn't": "might not",
"must've": "must have",
"mustn't": "must not",
"needn't": "need not",
"oughtn't": "ought not",
"shan't": "shall not",
"sha'n't": "shall not",
"she'd": "she would",
"she'll": "she will",
"she's": "she is",
"should've": "should have",
"shouldn't": "should not",
"that'd": "that would",
"that's": "that is",
"there'd": "there had",
"there's": "there is",
"they'd": "they would",
"they'll": "they will",
"they're": "they are",
"they've": "they have",
"wasn't": "was not",
"we'd": "we would",
"we'll": "we will",
"we're": "we are",
"we've": "we have",
"weren't": "were not",
"what'll": "what will",
"what're": "what are",
"what's": "what is",
"what've": "what have",
"where'd": "where did",
"where's": "where is",
"who'll": "who will",
"who's": "who is",
"won't": "will not",
"wouldn't": "would not",
"you'd": "you would",
"you'll": "you will",
"you're": "you are"
}


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
df_clean = clean_text_df(df, ['review_body','review_headline'])

# Define connection string
server = '192.168.0.52,1433'
database = 'master'
username = 'methmi'
password = 'Welcome123#'
driver= '{ODBC Driver 17 for SQL Server}'
cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

#cursor.execute('DROP TABLE review_watches')

# Create table
cursor = cnxn.cursor()
cursor.execute("""CREATE TABLE review_watches (review_id VARCHAR(255), star_rating INT, helpful_votes INT, total_votes INT, vine VARCHAR(255), verified_purchase VARCHAR(255), review_headline VARCHAR(255), review_body VARCHAR(MAX), review_date DATETIME)""")

# Convert DataFrame to list of tuples
values = [tuple(x) for x in df_clean.values]

# Insert data
insert_query = '''INSERT INTO review_watches (review_id, star_rating, helpful_votes, total_votes, vine
                        , verified_purchase, review_headline, review_body, review_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''cursor.executemany(insert_query, values)

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

# View dataframe
# visualize.head()

def sentiments(rating):
    if (rating == 5) or (rating == 4):
        return "Positive"
    elif rating == 3:
        return "Neutral"
    elif (rating == 2) or (rating == 1):
        return "Negative"


def plot_reviews(reviews_all):
    # Create new column with value ['Positive', 'Neutral', 'Negative']
    visualize["sentiment_category"] = visualize["star_rating"].apply(sentiments)

    # Get the sentiment score for each review
    reviews_all['sentiment'] = reviews_all['review_body'].apply(lambda x: TextBlob(x).sentiment.polarity)

    # Get the mean rating of the product
    mean_rating = reviews_all['star_rating'].mean()

    # Plot the histogram of the stars / ratings given by the customers
    plt.hist(reviews_all['star_rating'], bins=range(7))
    plt.savefig('C:\\Users\\donyo\\OneDrive\\Documents\\images\\navy/histogram.png')
    plt.close()
   
    # Get the count of the number of reviews scraped
    num_reviews = len(reviews_all)

    # Create a bar chart for verified_purchase and star_rating
    counts_verified_purchase = df.groupby(['star_rating', 'verified_purchase']).count()['review_id'].unstack()
    ax = counts_verified_purchase.plot.bar()
    ax.set_xlabel('Star rating')
    ax.set_ylabel('Count')
    ax.legend(title='Verified purchase', loc='upper left')
    

    # Create a bar chart for text review length and star_rating
    visualize['review_length'] = visualize['review_body'].apply(len)
    df_grouped = visualize.groupby('star_rating').mean()['review_length']

    # Plot the bar chart
    plt.figure(figsize=(10, 8))
    plt.bar(df_grouped.index, df_grouped.values)
    plt.title('Average Review Length by Star Rating')
    plt.xlabel('Star Rating')
    plt.ylabel('Average Review Length')
    plt.savefig('C:\\Users\\donyo\\OneDrive\\Documents\\images\\navy/Bar_Chart.png')
    plt.close()

    # Plot frequent words for all review
    cloud = wordcloud.WordCloud(background_color='gray', max_font_size=60, relative_scaling=1).generate(' '.join(df.review_body))

    plt.figure(figsize=(10, 8))
    plt.imshow(cloud, interpolation='bilinear')
    plt.axis('off')
    plt.title(f'Wordcloud for all Star Ratings')
    plt.savefig('C:\\Users\\donyo\\OneDrive\\Documents\\images\\navy/cloud.png')
    plt.close()

    # Plot frequent words for all rating 1

    df_1 = visualize[visualize['star_rating'] == 1]

    cloud_1 = wordcloud.WordCloud(background_color='white', max_font_size=60, relative_scaling=1).generate(' '.join(df_1.review_body))

    plt.figure(figsize=(10, 8))
    plt.imshow(cloud, interpolation='bilinear')
    plt.axis('off')
    plt.title(f'Wordcloud for Rating 1')
    plt.savefig('C:\\Users\\donyo\\OneDrive\\Documents\\images\\navy/cloud1.png')
    plt.close()

    # Plot frequent words for all rating 2

    df_2 = visualize[visualize['star_rating'] == 2]

    cloud_2 = wordcloud.WordCloud(background_color='white', max_font_size=60, relative_scaling=1).generate(' '.join(df_2.review_body))

    plt.figure(figsize=(10, 8))
    plt.imshow(cloud, interpolation='bilinear')
    plt.axis('off')
    plt.title(f'Wordcloud for Rating 2')
    plt.savefig('C:\\Users\\donyo\\OneDrive\\Documents\\images\\navy/cloud2.png')
    plt.close()

    # Plot frequent words for all rating 3

    df_3 = visualize[visualize['star_rating'] == 3]

    cloud_3 = wordcloud.WordCloud(background_color='white', max_font_size=60, relative_scaling=1).generate(' '.join(df_3.review_body))

    plt.figure(figsize=(10, 8))
    plt.imshow(cloud, interpolation='bilinear')
    plt.axis('off')
    plt.title(f'Wordcloud for Rating 3')
    plt.savefig('C:\\Users\\donyo\\OneDrive\\Documents\\images\\navy/cloud3.png')
    plt.close()

    # Plot frequent words for all rating 4

    df_4 = visualize[visualize['star_rating'] == 4]

    cloud_4 = wordcloud.WordCloud(background_color='white', max_font_size=60, relative_scaling=1).generate(' '.join(df_4.review_body))

    plt.figure(figsize=(10, 8))
    plt.imshow(cloud, interpolation='bilinear')
    plt.axis('off')
    plt.title(f'Wordcloud for Rating 4')
    plt.savefig('C:\\Users\\donyo\\OneDrive\\Documents\\images\\navy/cloud4.png')
    plt.close()

    # Plot frequent words for all rating 5

    df_5 = visualize[visualize['star_rating'] == 5]

    cloud_5 = wordcloud.WordCloud(background_color='white', max_font_size=60, relative_scaling=1).generate(' '.join(df_5.review_body))

    plt.figure(figsize=(10, 8))
    plt.imshow(cloud, interpolation='bilinear')
    plt.axis('off')
    plt.title(f'Wordcloud for Rating 5')
    plt.savefig('C:\\Users\\donyo\\OneDrive\\Documents\\images\\navy/cloud5.png')
    plt.close()

    # Plot sentiment category
    cnt_sentiment = visualize['sentiment_category'].value_counts()
    plt.savefig('C:\\Users\\donyo\\OneDrive\\Documents\\images\\navy/sentiment_category.png')
    plt.close()

    # Plot a pie chart of the sentiment categories

    plt.figure(figsize=(10, 8))
    labels = ['Positive', 'Neutral', 'Negative']
    sizes = [cnt_sentiment['Positive'], cnt_sentiment['Neutral'], cnt_sentiment['Negative']]
    colors = ['yellowgreen', 'gold', 'lightskyblue']
    plt.pie(sizes, labels=labels, colors=colors,
            autopct='%1.1f%%', startangle=140)
    plt.axis('equal')
    plt.savefig('C:\\Users\\donyo\\OneDrive\\Documents\\images\\navy/pie_chart.png')
    plt.close()

    # plot review length distribution

    plt.figure(figsize=(10, 8))
    plt.hist(visualize['review_length'], bins=100, color='skyblue')
    plt.xlabel('Review Length')
    plt.ylabel('Count')
    plt.title('Review Text Length Distribution')
    plt.savefig('C:\\Users\\donyo\\OneDrive\\Documents\\images\\navy/review_length_distribution.png')
    plt.close()
   
client.close()
