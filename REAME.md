# Prefect-Test

This repository is testing prefect as a solution for a problem in an event based architecture

# The Problem
When raw data is stored in aws s3 and has to be processed

# Solution
## AWS
I created an s3 event-notification which publishes a message on an SQS queue whenever a .csv file appears in a specific folder in the bucket.
Further I implemented a SNS topic for publishing a result message. This sends an email to myself but might as well publish to multiple queues itself to trigger further processing in a fan out session.