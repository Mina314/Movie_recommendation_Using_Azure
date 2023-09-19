##  Getting the data ##
ratings_filename="dbfs:/mnt/Files/Validated/ratings.csv"
ratings_filename="dbfs:/mnt/Files/Validated/movies.csv"

# -----------------------

## Data Exploration and analysis
# Create 2 dataframes for the analysis which will make the visualization with Databricks display function pretty straightforward
# 1. movies_based_on_time: movie_id, name, Year
# 2. movies_based_on_genres:  movie_id, name_with_year, one_genre

from pyspark.sql.types import *

#movies.csv
movies_with_genres_df_schema = StructType(
    [StructField('ID', IntegerType()),
     StructField('title', StringType()),
     StructField('genres', StringType())]
)
movies_df_schema = StructType(
    [StructField('ID', IntegerType()),
     StructField('title', StringType())]
) # drop the genres and transform the df to include the Year later
 
#Creating the dataframes
movies_df = sqlContext.read.format('com.databricks.spark.csv').optioins(header=True, inferSchema=False)
.schema(movies_df_schema).load(movies_filename)

movies_with_genres_df = sqlContext.read.format('com.databricks.spark.csv').optioins(header=True, inferSchema=False)
.schema(movies_with_genres_df_schema).load(movies_filename)

# -----------------------
## Inspecting the DataFrame before the transformations
movies_df.show(4,truncate=False) # this will also be used for the collabrative filtering
movies_with_genres_df.show(4,truncate=False)

# transforming the DataFrames
from pyspark.sql.functions import split, regexp_extract
movies_with_year_df = movies_df.select('ID', 'title', regexp_extract('title', r'\((\d+)\)', 1).alias('year'))

# -----------------------
## DataFrames after Transformation
movies_with_year_df.show(4,truncate=False)

# -----------------------
## Use the built-in functions of Databricks for some insights
# count number of movies by years
display(movies_with_year_df.groupBy('years').count().orderBy('count',ascending=False))

# -----------------------
## Ratings
# drop the timestamp column
ratings_df_schema = StructType(
    [StructField('userId', IntegerType()),
     StructField('movieId', IntegerType()),
     StructField('rating', DoubleType())]
)

# Creat the df
ratings_df = sqlContext.read.format('com.databricks.spark.csv').optioins(header=True, inferSchema=False)
.schema(ratings_df_schema).load(ratings_filename)
ratings_df.show(5)

ratings_df.cache()
movies_df.cache()

# -----------------------
## Global Popularity - discard the movies where the count of ratings is < 500

from pyspark.sql import functions as F 

movie_ids_with_avg_ratings_df = ratings_df.groupBy('movieId').agg(F.count(ratings_df.rating).alias("count"),
                                                                  F.avg(ratings_df.rating).alias("average"))
print('movie_ids_with_avg_ratings_df:')
movie_ids_with_avg_ratings_df.show(4, truncate=False)

movie_names_with_avg_ratings_df=movie_ids_with_avg_ratings_df.join(movies_df,F.col('movieID') == F.col('ID')).drop('ID')
print('movie_names_with_avg_ratings_df:')
movie_names_with_avg_ratings_df.show(4, truncate=False)

movies_with_morethan_500_ratings = movie_names_with_avg_ratings_df.filter(movie_names_with_avg_ratings_df['count'] >=
                                                                          500).orderBy('average', ascending=False)
movies_with_morethan_500_ratings.show(truncate = False)

# -----------------------
## Split in Train, test and validation dataset
# 60% training, 20% validation, 20% test
seed = 4
(split_60_df, split_v_20_df, split_t_20_df) = ratings_df.randomSplit([0.6,0.2,0.2], seed)

# cache for performance
trainging_df = split_60_df.cache()
validation_df = split_v_20_df.cache()
test_df = split_t_20_df.cache()

print('Training: {0}, validation: {1}, test: {2}\n'.format(
    trainging_df.count(), validation_df.count(), test_df.count())
)

trainging_df.show(4,truncate=False)
validation_df.show(4,truncate=False)
test_df.show(4,truncate=False)

# -----------------------
## Alternating Least Square (ALS)
from pyspark.ml.recommendation import ALS
als = ALS()

# reset the parameters for the ALS object
als.setPredictionCol("prediction")\
    .setMaxIter(5)\
    .setSeed(seed)\
    .setRegParam(0.1)\
    .setUserCol("userId")\
    .setItemCol("movieId")\
    .setRatingCol("rating")\
    .setRank(8)
my_rating_model = als.fit(trainging_df)

# Root mean square deviation - differences between values predicted by a model and the values observed
from pyspark.ml.evaluation import RegressionEvaluator

from pyspark.sql.funcations import col 
reg_eval = RegressionEvaluator(predictionCol="prediction", labelCol="rating", metricName="rmse")
my_predict_df = my_ratings_model.transform(test_df)

# Remove Null 
predicted_test_my_ratings_df = my_predict_df.filter(my_predict_df.prediction != float('nan'))

test_RMSE_my_ratings = reg_eval.evalute(predicted_test_my_ratings_df)
print('The model had a RMSE on the test set of {0}'.format(test_RMSE_my_ratings))
dbutils.widgets.text("input", "5","")
ins=dbutils.widgets.get("input")
uid=int(ins)
ll=predicted_test_my_ratings_df.filter(col("userId") == uid)

# output
MovieRec = ll.join(movies_df, F.col("movieID") == F.col("ID")).drop("ID").select("title").take(10)
l = dbutils.notebook.exit(MovieRec)



