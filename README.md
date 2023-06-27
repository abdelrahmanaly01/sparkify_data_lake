# sparkify ELT and Data Lake using  AWS EMR and S3
================
## summary :
================
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I am tasked with building an ELT pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ELT pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.


==============================
## how to use :
==============================
on AWS spark cluster :
    1- you will need to provide valid access_key and secret_access_key in dl.cfg file 
    2- run the which spark-submit to know the directory for spark submissions
    3- use the following command to run the script
        >> <spark-submit-directory>  --master yarn ./etl.py

localy (not recomended):
    -just run the following command
        >> python etl.py
=======================
## files Utilization :
=======================

**etl.py:** loads the data from S3 to the spark DataFrames process them in AWS Spark cluster and save them back to S3 in parquet format.

**dl.cfg:** Contains the credintials for your EMR cluster connection AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY.

