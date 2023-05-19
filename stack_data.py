# Databricks notebook source
# MAGIC %pip install requests

# COMMAND ----------

import requests


# Define the API endpoint URL
api_url = 'https://api.stackexchange.com/2.3/'

# Define the parameters for each endpoint
params = {
    'tags': {
        'order': 'desc',
        'sort': 'popular',
        'site': 'stackoverflow'
    },
    'answers': {
        'order': 'desc',
        'sort': 'activity',
        'site': 'stackoverflow'
    },
    'questions': {
        'order': 'desc',
        'sort': 'activity',
        'site': 'stackoverflow'
    }
}

# Function to fetch data from an endpoint
def fetch_data(endpoint, endpoint_params):
    # Make a GET request to the API endpoint
    response = requests.get(api_url + endpoint, params=endpoint_params)

    if response.status_code == 200:
        # Extract the data from the response
        data = response.json()

        # Check if the response contains a 'items' key
        if 'items' in data:
            return data['items']
        else:
            print(f"No data found for {endpoint}")
    else:
        print(f"Error fetching data for {endpoint}: {response.status_code}")

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Fetch data for each endpoint and convert it to a DataFrame
dataframes = {}
for endpoint, endpoint_params in params.items():
    items = fetch_data(endpoint, endpoint_params)
    if items:
        df = spark.createDataFrame(items)
        dataframes[endpoint] = df

# Store the DataFrames in tables
for endpoint, df in dataframes.items():
    df.createOrReplaceTempView(f"temp_{endpoint}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {endpoint} AS SELECT * FROM temp_{endpoint}")
    print(f"Data stored in the table '{endpoint}'")



# COMMAND ----------

# Store the DataFrames in Delta format with partitioning in S3
for endpoint, df in dataframes.items():
    delta_location = f"s3a://bucket_name/{endpoint}_delta"
    df.write.partitionBy("creation_date").format("delta").mode("overwrite").save(delta_location)
    print(f"Data stored in Delta format at '{delta_location}' with partitioning on 'creation_date'")

# COMMAND ----------

spark.sql("SELECT * FROM tags").show()

# COMMAND ----------

spark.sql("SELECT * FROM answers").show()

# COMMAND ----------

spark.sql("SELECT * FROM questions").show()

# COMMAND ----------

#retrieve the top 10 most popular tags by count
spark.sql("SELECT name, count FROM tags ORDER BY count DESC LIMIT 10").show()

# COMMAND ----------

#calculate the average count of tags
spark.sql("SELECT AVG(count) AS avg_count FROM tags").show()

# COMMAND ----------

#number of accepted answers
accepted_answers = spark.sql("SELECT COUNT(*) FROM answers WHERE is_accepted = true").collect()[0][0]
display(accepted_answers)

# COMMAND ----------

#percentage of answers with a score greater than 10
total_answers = spark.sql("SELECT COUNT(*) FROM answers").collect()[0][0]
high_score_percentage = (spark.sql("SELECT COUNT(*) FROM answers WHERE score > 10").collect()[0][0] / total_answers) * 100
display(high_score_percentage)

# COMMAND ----------

#top 5 users with the highest scores
top_users = spark.sql("SELECT owner['user_id'] as user_id, SUM(score) as total_score FROM answers GROUP BY owner['user_id'] ORDER BY total_score DESC LIMIT 5")
display(top_users)

# COMMAND ----------

#number of answered questions
answered_count = spark.sql("SELECT COUNT(*) AS answered_count FROM questions WHERE is_answered = true").first()["answered_count"]
display(answered_count)

# COMMAND ----------

#questions with the highest view count
top_viewed_questions = spark.sql("SELECT question_id, title, view_count FROM questions ORDER BY view_count DESC LIMIT 5")
display(top_viewed_questions)

# COMMAND ----------


