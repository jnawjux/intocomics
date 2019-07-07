import pandas as pd
from ..recommend import get_recommendations

from flask import Flask, request, render_template

app = Flask(__name__, static_url_path="")

item_factors_df = pd.read_json('../../data/als_item_factor_details.json')

@app.route('/')
def index():
    """Return the main page."""
    return render_template('index.html', comic_titles={})


@app.route("/results", methods = ["GET", "POST"] )
def process_form():
    """Return comic book recommendations from user input"""
    # Create dataframe of user reviews
    reviews = []
    for k, v in request.form.items():
        rating = {'rating': int(v), 'item_id': int(k)}
        reviews.append(rating)
    user_df = pd.DataFrame(reviews)

    # Get recommendations
    user_recommendations = get_recommendations(item_factors_df, user_df)

    # Create dictionary of top ten comic titles and image
    top_recommendations = []
    for index, comic in user_recommendations.iterrows():
        image_url = 'images/comics/' + str(comic['item_id']) + '.jpg'
        amazon_url = 'http://asin.info/a/' + comic['asin']
        comic_recommendation = [comic['title'], image_url, amazon_url]
        top_recommendations.append(comic_recommendation) 

    return render_template('recommend.html', comic_titles = top_recommendations)
