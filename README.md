# KPL Java Sample Application

## Setup

If running in EC2, the KPL will automatically retrieve credentials from any associated IAM role.

If you are running this in a local machine, you need
+ AWS "credentials" file under your home directory

Linux
```
export AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID>
export AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY>
```

Windows
```
set AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID>
set AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY>

When session has expired, these variables have to be reset. First, existing variables have to be unset.

```
To unset
```
unset AWS_ACCESS_KEY_ID
unset AWS_SECRET_ACCESS_KEY
```

## Run the Sample

After you've made the necessary changes, do a clean build:

```
mvn clean package
```

Then run the producer to put some data into your stream:

```
mvn exec:java -Dexec.mainClass="com.kinesis.producer.TwitterProducerMain"
```

Finally run the consumer to retrieve that data:

```
mvn exec:java -Dexec.mainClass="com.kinesis.consumer.TwitterConsumerMain"
```
