import random
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import Pipeline
import pickle
from flask import Flask, request, render_template, jsonify


with open('spam_model.pkl', 'rb') as f:
    model = pickle.load(f)
app = Flask(__name__, static_url_path="")

@app.route('/')
def index():
    """Return the main page."""
    return render_template('indexy.html')


@app.route('/predict', methods=['GET', 'POST'])
def predict():
    """Generate new sentences and add to origianl sentence."""
    data = request.json
    prediction = model.predict_proba([data['user_input']])
    return jsonify(new_text)

