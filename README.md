# Quick Demo Promotions in Real time from tweets
In order to complete this demo, we used a kafka cluster (confluent) and a python, both as a microservices.
Set environment, please run our python jupyter container:
``` bash
docker run -d --name jupyter3 -e "TZ=America/Lima" -p 1002:8888 jupyter/datascience-notebook
```
Then create the kafka cluster
``` bash
docker-compose -f cp-all-in-one.yml up -d
```
Now, clone or download this repo and move to the container, you can use git clone inside the container with an docker exec before or move files using docker cp command.

### Considerations:
* Be sure that jupyter container has same docker network with kafka cluster, if not apply docker diconnect and connect, for have a common docker network.
* Create a gmail account and enable send mails from thirds apps.
* Also for connect to twitter you will request a [twitter developer account](https://developer.twitter.com/en). this last credentials you will need to replace in the producer and consumer app(.py files)
* All repo files must be in the same folder

## Workflow
1. Open twitter and tweet some like: Hello #FrankoPromotion, I'm waiting my promotions franko.95.12@gmail.com please call me 922138798 :D.
2. Then go to kafka http://localhost:9021, and create a topic named: **coedata_topic** and from it create a stream named **COEDATA_stream**. 
3. Now, Once you setted the credentials to .py files , run the producerAvroTwitterKafka.py file (if prefer open and copy code using .ipynb notebook files in jupyter). Jupyter is running on : http://localhost:1002
4. This producer execution will connect to twitter and query the terms defined in the consumer for us #FrankoPromotion and will connect to kafka for sink the events in **coedata_topic** using an avro schema.
5.Then please go to kafka  http://localhost:9021 and execute the next sentece that uses ksql for transform data in real time.
``` bash
CREATE STREAM COEDATA_stream_output WITH (KAFKA_TOPIC='COEDATA_STREAM_TOPIC',VALUE_FORMAT='JSON') AS 
SELECT 
    source,
    full_text,
    user_name,
    user_followers,
    user_friends,
    user_created_at,
    user_years_ago,
    user_os,
    id_str,
case when CAST(user_years_ago AS INT) >10 then 'cat1' 
     when CAST(user_followers AS INT) >100 then 'cat2' 
     when user_os = 'iPhone' then 'cat3' 
     else 'catdefault'end as custom_message
FROM COEDATA_stream;
```
6. At this point this last sentence was resolved a couple of transformations and sinked events to **COEDATA_STREAM_TOPIC** topic, the one who our consumer will use.
Finally open the file in the jupyter container consumerJsonTwitterKafka.py or .ipynb if you prefer and run it.
7. You will have some promotions in your mail or whatsapp(check twilio -not free).


