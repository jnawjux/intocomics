import pandas as pd
import pyspark
import pyspark.sql.functions as F
from pyspark.ml.recommendation import ALS
from pyspark.sql.types import StringType, IntegerType
from IPython.display import clear_output

def create_spark_review_df(filename="data/all_reviews_fixed_titles.json"):
    """
    Return Spark dataframe, default to primary cleaned reviews datafame
    Args:
        file (optional): give file to be read in 
    Returns:
        spark_df: Spark dataframe of file
    """
    # Instantiate Spark & load reviews
    spark = (pyspark.sql.SparkSession.builder
    .master("local")
    .getOrCreate())
    
    spark_df = spark.read.json(filename)
    return spark_df

def run_model_on(spark_df):
    """Run Alternating Least Squares Model on Spark Dataframe
    Args:
        spark_df: input Spark Dataframe
    Returns:
        als_model.fit: """
    als = ALS(rank=5, regParam=0.01, maxIter=20,
    userCol='user_id', itemCol='item_id', 
    ratingCol='overall', nonnegative=True)
    
    model = als.fit(spark_df)
    return model

def get_comic_recommendations(spark_df, model, new_user=101):
    """Take in model and dataframe and return the recommendations that are comic books/graphic novels
    Args:
        spark_df: Spark Dataframe with item_id, user_id, rating, and titles
    Returns:
        comic_titles: list of comic book titles and item_id
    """
    user_recommend = model.recommendForAllUsers(30)
    recs_for_user = user_recommend.where(user_recommend.user_id == new_user).take(1)
    
    all_comics = [reco[0] for reco in recs_for_user[0]['recommendations']\
                  if str(reco[0]).endswith('22') ]
    comic_titles = list(set(spark_df.filter(F.col('item_id')\
                                       .isin(all_comics))\
                                       .select(['item_id', 'title']).collect()))

    return comic_titles

def get_user_reviews_testing(new_user=101):
    """Take user input and create dataframe added for recommending, built for testing in Jupyter Notebook
    Args:
        None
    Returns:
        pd.DataFrame(reviews): Pandas dataframe of users reviews from inputs
    """
    
    all_reviews = create_spark_review_df()

    # Instantiate Spark & load reviews
    spark = (pyspark.sql.SparkSession.builder
    .master("local")
    .getOrCreate())

    # Make a dataframe of just movies
    query = """
        SELECT 
            DISTINCT CAST(item_id as string) as item_id
        ,   title
        ,   count
        FROM 
            table
        WHERE 
            item_id LIKE '%44'"""

    all_reviews.createOrReplaceTempView('table')

    get_movies = spark.sql(query).toPandas()

    # Sort dataframe by review count, take a random sample from the top 500 reviewed
    get_movies.sort_values('count', ascending=False, inplace=True)
    movie_rand_sample = get_movies[:500].sample(n=100)

    reviews = []

    # Give a user input and movie title and take in score
    for index, movie in movie_rand_sample.iterrows():
        print(movie['title'])
        rating = input("How would you rate this movie? (0-5, OR type 'skip'): ")
        # If user has not seen, can enter skip instead
        if rating == 'skip':
            clear_output()
            continue
        # Creating dictionary of review and adding to reviews
        else:
            movie_rating = {'user_id': 101, 'overall': int(rating),
                            'item_id': int(movie['item_id']), 'title': movie['title']
                            }
            reviews.append(movie_rating)
            clear_output()
            if len(reviews) >=10:
                return pd.DataFrame(reviews)
            else:
                continue

def get_recommendations_testing(new_user_df, new_user=101):
    """Get recommendations for new user, built for testing in Jupyter Notebook
    Args:
        new_user: id given for new user, defaults to standard 101
        new_user_df: Pandas dataframe with user reviews, as generated from get_user_reviews function
    Returns:
        Prints top three recommended comics for new user
    """
    spark = (pyspark.sql.SparkSession.builder
    .master("local")
    .getOrCreate())

    # Combine user reviews with others and prep for modeling
    new_user_spark = spark.createDataFrame(new_user_df)

    all_reviews = create_spark_review_df().select([F.col("item_id").cast(IntegerType()), F.col("overall"),
                                                   F.col("title"), F.col("user_id").cast(IntegerType())
                                                  ])

    ratings_all = all_reviews.union(new_user_spark)
                  
    # Create ALS model
    als_model = run_model_on(ratings_all)
    
    # Get recommendations for user and only return those that are comics & top three
    comic_titles = get_comic_recommendations(ratings_all, model=als_model)

    for comic in comic_titles:
        print(comic)