#!/usr/bin/env python

"""
Generates a stream to Kafka from a csv file.
"""

import argparse
import csv
import json
import sys
import time
from dateutil.parser import parse
from confluent_kafka import Producer
import socket
import readXML

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg.value()), str(err)))
    else:
        print("Message produced: %s" % (str(msg.value())))


def main():
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument('filename', type=str,
                            help='Time series csv file.')
        parser.add_argument('topic', type=str,
                            help='Name of the Kafka topic to stream.')
        parser.add_argument('--speed', type=float, default=1, required=False,
                            help='Speed up time series by a given multiplicative factor.')
        args = parser.parse_args()

        topic = args.topic
        p_key = args.filename

        conf = {'bootstrap.servers': "localhost:9092",
                'client.id': socket.gethostname()}
        producer = Producer(conf)

        rdr = csv.reader(open(args.filename))
        next(rdr)  # Skip header
        firstline = True

        while True:

            try:

                if firstline is True:
                    line1 = next(rdr, None)
                    city, value = line1[0], line1[1:]
                    # Convert csv columns to key value pair
                    result = {}
                    result[city] = value
                    # Convert dict to json as message format
                    jresult = json.dumps(result)
                    firstline = False
                                    
                    producer.produce(topic, key=p_key, value=jresult, callback=acked)

                else:
                    line = next(rdr, None)
                    time.sleep(args.speed)
                    city, value = line[0], line[1:]
                    result = {}
                    result[city] = value
                    jresult = json.dumps(result)

                    producer.produce(topic, key=p_key, value=jresult, callback=acked)

                producer.flush()

            except TypeError:
                sys.exit()
            except KeyboardInterrupt:
                sys.exit()


if __name__ == "__main__":
    readXML.InitPaths()
    main()
