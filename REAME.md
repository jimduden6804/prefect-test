# Prefect-Test

This repository is testing prefect as a solution for a problem in an event based architecture

# The Problem
When raw data is stored in aws s3 and has to be processed

# Solution
## AWS
I created an s3 event-notification which publishes a message on an SQS queue whenever a .csv file appears in a specific folder in the bucket.
Further I implemented a SNS topic for publishing a result message. This sends an email to myself but might as well publish to multiple queues itself to trigger further processing in a fan out session.

# Usage
Since the code uses AWS Infrastructure, you need some credential management (e.g. awsume)

# Outlook
Event-Driven flow as proposed in [pin 8](https://docs.prefect.io/core/PINs/PIN-08-Listener-Flows.html) and [pin 14](https://docs.prefect.io/core/PINs/PIN-14-Listener-Flows-2.html) could make this more convenient. Instead of scheduling the task you could then use an integrated listener.