# recursive lambda

Experimental Kafka-Connect like Consumer with Self-invoking AWS Lambda

## Why?

Kafka Connect is a massive system that does the job, but maintaining it is difficult. Why not use lambda instead?

## How?

The idea is to configure a Lambda function on a regular cadence (daily, hourly or more) and 
as long as the Work Channel contains messages, it will continue to invoke itself, thus scaling in response to 
a large volume of messages.
