import datetime
from gcloud import pubsub
import sys

client = pubsub.Client(project=sys.argv[1])
topic = client.topic(sys.argv[2])

topic.publish('{ "this is" : "a body", "aaaa" : 123 }', client, thetimeisnow=str(datetime.datetime.utcnow()), param1="hello", param2="world")
