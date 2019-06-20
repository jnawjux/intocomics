# Comic Sherpa
#### <em>Your guide to getting into comic books</em>
### Business Understanding
A passion of mine is comic books. As much as I love the medium, the industry at large is often looked over in favor of the next big movie or tv show using their stories or characters. Further, there are a number of great stories and worlds done by smaller artists/writers that are waiting for a chance to shine. My goal is to create an app where people will be able to match their movie and television preferences to comic books, helping open up a new world of entertainment for them. 

### Data Understanding
My current data source is a large set of Amazon reviews (over 10GB, from 1995-2008). I plan on trying to pull in other sources as needed for additional context as needed.

### Data Preparation
My goal is to extract users that have rated both movies/television and comic books. I will have to get some categorical information to help pull out the correct review categories from a large set of Books, and then find by users those who rated movies/television. I will need to relabel my data with new unique identifiers for both comic books and movies/television to help keep them seperate for processing later, and assign new user ids.

### Modeling
My current approach is to build a Spark ALS recommendation system model, using overlap between users who have rated both movies/television and comic books.  This will be the basis for my model to help make suggestions.  I am also considering doing some topic modeling with the review texts for comic books and then testing against it with reviews of movies, and seeing how that performs as well.  

### Evaluation
I plan to use standard evaluation metrics when training and testing my data, such as RMSE. I want to also try out Precision at K and Mean Average Precision. I would like to as well, if I have the time, do a little quality testing by having actual users attempt to use the system and see how it performs. 

### Deployment
The model will be deployed as a Flask app that will allow users to interactively rate movies and return a set of the top comic books that the system would suggest for them. 

<b>User story:</b> Bruce just watched <em>Avengers: Endgame</em> and was blown away.  He loved the rich story telling, incredible visuals, and unique world it created. He has of course heard that many of these stories come from comic books, but has never picked one up before. Bruce has many other interests as well, and is a little hesitant to read comics, since he doesn't want to just read stories about superheroes. There is no comic book shops in his area, and his next best hope to find something he may like is reading through reviews or website lists to hopefully find something interesting.
<br/><br/>
Bruce goes to Comic Sherpa and is able to take his already existing preferences for different television shows and movies and be given a curated list of comics he might want to check out.  Bruce then follows links through the page to purchase and further explore the options offered to him.  
