1. Kafka Producer for Ad Impressions (Python):

Python
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Assuming ad_impressions is a list of dictionaries containing impression data
for impression in ad_impressions:
  producer.send('ad_impressions', json.dumps(impression).encode('utf-8'))

producer.flush()
