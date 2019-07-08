import pandas as pd
import numpy as np
from IPython.display import clear_output


def get_new_user_matrix(item_df, user_df, rank=50):
    """
    Take in a new users ratings, align with the existing item
    factors,and return the user factors
    Args:
        item_df: item factors dataframe resulting from a Spark
         Alternating Least Squares (ALS) Model
        user_df: Pandas dataframe with item_ids and ratings
    Returns:
        new_user_matrix: np.array with calculated new users factors.
    """
    # User ratings array
    all_ratings_array = np.array((user_df.rating.tolist(),)).T

    # Item factors array
    all_items_array = np.zeros(shape=(user_df.shape[0], rank))

    for index, item in user_df.iterrows():
        all_items_array[index, :] = np.array(
            item_df.loc[item_df["item_id"] == item["item_id"],
                        "features"].item()
        )

    # Least squares solution to get user features
    new_user_matrix = np.linalg.lstsq(all_items_array,
                                      all_ratings_array,
                                      rcond=None)

    # New users matrix!
    new_user_matrix = new_user_matrix[0].reshape((50,))

    return new_user_matrix


def get_user_reviews_testing(items_df):
    """Take user input and create dataframe added for recommending,
         built for testing in Jupyter Notebook
    Args:
        None
    Returns:
        pd.DataFrame(reviews): Pandas dataframe of users reviews from inputs
    """
    # Load data and get random sample of movies with more than 100 reviews
    movie_rand_sample = items_df[
        (items_df["item_id"].astype(str).str.endswith("44")) &
        (items_df["count"] > 50)
    ].sample(n=25)
    reviews = []
    # Give a user input and movie title and take in score
    for index, movie in movie_rand_sample.iterrows():
        print(movie["title"])
        rating = input(
            "How would you rate this movie? (0-5, OR type 'skip'): "
            )
        # If user has not seen, can enter skip instead
        if rating == "skip":
            clear_output()
            continue
        # Creating dictionary of review and adding to reviews
        else:
            movie_rating = {"rating": int(rating), "item_id": movie["item_id"]}
            reviews.append(movie_rating)
            clear_output()
            if len(reviews) >= 10:
                return pd.DataFrame(reviews)
            else:
                continue


def get_recommendations(item_factors_df, new_user_df):
    """Get recommendations for new user,
        built for testing in Jupyter Notebook
    Args:
        new_user_df: Pandas dataframe with user reviews,
        as generated from get_user_reviews function
    Returns:
        Prints top three recommended comics for new user
    """
    user_matrix = get_new_user_matrix(item_factors_df, new_user_df)

    item_factors_df["new_user_predictions"] = item_factors_df["features"]\
        .apply(
            lambda x: np.dot(x, user_matrix)
            )

    top_five_comics = item_factors_df.loc[
        item_factors_df["item_id"].astype(str).str.endswith("22"),
        ["item_id", "title", "asin", "new_user_predictions"],
    ].sort_values("new_user_predictions", ascending=False)[:5]

    return top_five_comics
