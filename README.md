# What was the objective of this exercise?
To learn about MongoDB, by creating a Datamart from the file DataSemilla.csv, not unlike a previous SQL exercise made. Additionally, some graphs and insights were to be made afterwards. 

# What were the results?
I was able to:
 * Import the data using a bash script and mongoimporter.
 * Clean the data using pymongo.
 * Create the dimensions for all the data. 
 * Create the fact table for the dates only. 

# What did I learn during this exercise? 

* To import data with the CLI tool mongoimporter. 
* To install and create a MongoDB server in localhost.
* To create multistage pipelines and queries in MongoDB. Some of the used pipeline operators were:
    * dateFromString
    * addFields
    * unset
    * out
    * group
    * project
    * unwind
    * match
    * expr
    * eq
    * lookup
    * facet
    * concatArrays
    * multiple date operations.
* Access fields of documents or intermediate documents using the dollar sign operator ($).
* To create collections in MongoDB, and define schemas. 
* To make queries in order to insert documents from other documents.
* To simulate an inner join in MongoDB.
* That using MongoDB to create a datamart exactly like I would do it in SQL is not the best idea. 
