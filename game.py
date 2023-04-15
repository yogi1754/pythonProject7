import urllib.request
import gzip
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

# Download and extract the dataset
url = 'https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Gift_Card_v1_00.tsv.gz'
filename = 'amazon_reviews_us_Gift_Card_v1_00.tsv.gz'
urllib.request.urlretrieve(url, filename)
with gzip.open(filename, 'rt', encoding='utf-8') as f:
    reader = csv.DictReader(f, delimiter='\t')
    for i, row in enumerate(reader):
        if i == 1010:
            break
        collection.insert_one(json.loads(json.dumps(row)))

# Load data from MongoDB into a Pandas DataFrame
df = pd.DataFrame(list(collection.find()))

# Clean and normalize data
df['review_date'] = pd.to_datetime(df['review_date'])
df['star_rating'] = pd.to_numeric(df['star_rating'], errors='coerce')
df = df.dropna()

# Perform data transformation
df['log_rating'] = np.log(df['star_rating'])

# Perform linear regression on the data
model = LinearRegression()
days_since_start = (df['review_date'] - df['review_date'].min()).dt.days
X = days_since_start.values.reshape(-1, 1)
y = df['log_rating']
model.fit(X, y)
print("Coefficients:", model.coef_)
print("Intercept:", model.intercept_)

# Visualize the data using a scatter plot
plt.scatter(df['review_date'], df['log_rating'])
plt.title("Logarithm of Rating Over Time")
plt.xlabel("review_date")
plt.ylabel("Logarithm of Rating")

# Create a histogram of star ratings
plt.hist(df['star_rating'], bins=5)
plt.title("Histogram of Star Ratings")
plt.xlabel("Star Ratings")
plt.ylabel("Count")

plt.show()


# Create a bar chart of the count of reviews by gift card category
category_counts = df.groupby('product_category')['review_id'].count()
plt.bar(category_counts.index, category_counts.values)
plt.title("Number of Reviews by Gift Card Category")
plt.xlabel("Gift Card Category")
plt.ylabel("Number of Reviews")
plt.xticks(rotation=90)

plt.show()


# Create a scatter plot matrix of the numerical variables in the dataset
sns.pairplot(df.select_dtypes(include=[np.number]))
plt.suptitle("Scatter Plot Matrix")
plt.show()


# Create a time series plot of the average star rating per month
df['year_month'] = df['review_date'].dt.to_period('M')
monthly_avg_rating = df.groupby('year_month')['star_rating'].mean()
plt.plot(monthly_avg_rating.index.to_timestamp(), monthly_avg_rating.values)
plt.title("Average Star Rating per Month")
plt.xlabel("Month")
plt.ylabel("Average Star Rating")

plt.show()


# Add regression line to the plot
plt.plot(df['review_date'], model.predict(X), color='red')

# Create a heatmap of the correlation matrix
sns.heatmap(df.select_dtypes(include=[np.number]).corr(), annot=True, cmap='coolwarm')
plt.title("Correlation Matrix Heatmap")

plt.show()

# Create a boxplot of the star ratings
sns.boxplot(x='star_rating', data=df)
plt.title("Boxplot of Star Ratings")

plt.show()

# Create a violinplot of the star ratings
sns.violinplot(x='star_rating', data=df)
plt.title("Violinplot of Star Ratings")

plt.show()

# Close the MongoDB connection
client.close()
