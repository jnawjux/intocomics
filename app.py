import random
import pandas as pd
from selenium.webdriver import Chrome
import pyspark
import pyspark.sql.functions as F
from pyspark.sql import Row
from pyspark.ml.recommendation import ALS

from flask import Flask, request, render_template, jsonify

# Load Spark session
spark = (pyspark.sql.SparkSession.builder
.master("local")
.getOrCreate())

# Load and persist ratings set
all_reviews = spark.read.json('data/all_reviews_fixed_titles.json')
all_reviews.persist()

app = Flask(__name__, static_url_path="")

@app.route('/')
def index():
    """Return the main page."""
    return render_template('index.html', comic_titles={})


@app.route("/process", methods = ["GET", "POST"] )
def process_form():

    # Create dataframe of user reviews
    reviews = []
    for k, v in request.form.items():
        rating = {'user_id': 101, 'overall': int(v), 'item_id': int(k) }
        reviews.append(rating)
    user_df = spark.createDataFrame(Row(**x) for x in reviews)

    # Combine user reviews with others and prep for modeling
    ratings_all = all_reviews.select(['item_id','overall','user_id'])\
                             .union(user_df)

    # Create ALS model 
    als = ALS(rank=5, regParam=0.01, 
      userCol='user_id', itemCol='item_id', 
      ratingCol='overall', nonnegative=True)
    
    als_model = als.fit(ratings_all)
    
    # Get recommendations for user and only return those that are comics & top three
    user_recommend = als_model.recommendForAllUsers(50)
    recs_for_user = user_recommend.where(user_recommend.user_id == 101).take(1)
    all_comics = [reco[0] for reco in recs_for_user[0]['recommendations']\
                  if str(reco[0]).endswith('22') ]
    comic_titles = list(set(all_reviews.filter(F.col('item_id')\
                                       .isin(all_comics))\
                                       .select(['item_id', 'title']).collect()))

    top_recommendations = {}
    for comic in comic_titles:
        top_recommendations[comic[1]] = 'images/comics/' + str(comic[0]) + '.jpg'

    return render_template('index.html', comic_titles = top_recommendations)
