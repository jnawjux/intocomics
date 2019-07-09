# IntoComics
#### <em>Your guide to getting into comic books</em>
---

* [Amazon Review data exploratory data analysis](https://github.com/jnawjux/intocomics/blob/master/amazon_reviews_eda.ipynb)
* [Data preparation and modeling](https://github.com/jnawjux/intocomics/blob/master/data_prep_and_modeling.ipynb)
* [Web app development](https://github.com/jnawjux/intocomics/blob/master/web_app_development.ipynb) 
* [Presentation (Google Slides)](https://docs.google.com/presentation/d/17ZCj6XF-yz0qAAhKozr-Nzvy8R7UQNkzM-Hzz_ehBs0/edit?usp=sharing)
* [Live Web App](http://www.intocomics.naujoks.co)

### Business Understanding
A passion of mine is comic books. As much as I love the medium, the industry at large is often looked over in favor of the next big movie or tv show using their stories or characters. Further, there are a number of great stories and worlds done by smaller artists/writers that are waiting for a chance to shine. My goal is to create a recommendation system where people will be able to match their movie and television preferences to comic books, helping open up a new world of entertainment for them. 

### Data Understanding
To build my model, I used a large repository of Amazon reviews previously collected in a research project at the University of California, San Diego (~24GB of book and movies/tv reviews from 1996-2014, [more information here](http://jmcauley.ucsd.edu/data/amazon/links.html)). My goal was to use ratings from users who reviewed both comic books and movies/tv.

### Data Preparation
In order to extract the correct users and ratings, I had to spend a good amount of time learning and exploring this dataset. All of the comic books/graphic novels are lumped within all other book reviews with no shortcuts to pull them out. I started with a smaller amount of Amazon IDs (ASIN) for comic books from scraping Amazons bestseller pages. Based on data exploration, I found a pattern in the ids to help shorcut get a few large chunks of ids for comic books. With this set of ids, I found any corresponding reviews. I then took the reviewers in that set and found any that also had reviewed movies/tv. After removing items with less than 5 reivews, and dropping any data missing relevant metadata, I was working with ~84,000 reviews, with ~8,500 distinct users and ~7,400 items (~1,300 comic books/graphic novels, ~6,100 movies/tv).

### Modeling
My approach was to build a Alternative Least Squares (ALS) model to have a collaborative filtering recommender system. I treated both movies/tv and comic books equally as items. Using a matrix for all users and items, I created the model, but filtered based on type when getting top recommendations.  

### Evaluation
For evaluation, I optimized the performance of my model based on optimizing for Root Mean Squared Error (RMSE) and Mean Absolute Error (MAE). Tuning the parameters of my model did not prove to offer too much in terms of performance, but I settled on my best model using a rank of 50, regularization parameter at .1, and max number of iterations at 20. My best performing model has an RMSE of 1.18.  In general, I would hope to get that number under 1, but think this is a fairly good performance under my current scope. I am working under a larger assumption that each user's taste preferences (regardless of media) are the same, which in general might seem like a big leap, but with this performance actually seems to speak well to that point. 

### Deployment
I have created and am working to deploy to a website a web application version of this fucntional recommendation system.  The application gives the user a selection of movies to rate from 1 to 5 (currently a curated list of 60 movies that were most frequently rated, but leaving off all that are based on comic books or graphic novels to more cleanly drive the seperation of the two medium). Once they have rated one or more, it returns a listing of the top 5 comic book/ graphic novel recommendations with links to each product on Amazon.

### Next Steps
##### Model improvements:
* Explore adjustments to data to account for user and item bias.
* Gather more recent review data to improve and deepend my models ability.

##### Web app improvements:
* Add additional user features such as filtering and content for recommended comic books (Examples: filter by independent or major company published comics, display short description or review of item)
* Add more movies and television options to choose from for user to rate on.
