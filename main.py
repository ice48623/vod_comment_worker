#!/usr/bin/env python3
from pymongo import MongoClient
import os
import pika
import json
import logging


RABBIT_HOST = os.getenv('RABBIT_HOST', 'localhost')
MONGO_URL = os.getenv('MONGO_URL', 'localhost')
MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
MONGO_DB = os.getenv('MONGO_DB', 'my_db')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'my_collection')
mongoClient = MongoClient(f'mongodb://{MONGO_URL}', MONGO_PORT)
db = mongoClient[MONGO_DB]
collection = db[MONGO_COLLECTION]

LOG = logging
LOG.basicConfig(
    level=LOG.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
channel = connection.channel()
channel.queue_declare(queue='comment')

def update_vid_comment(data):
    vid_id = data['vid_id']
    comment = data['comment']

    collection.find_one_and_update(
        {'vid_id' : vid_id},
        { '$push' : {'vid_comments' : comment}}
    )

    return vid_id

def callback(ch, method, properties, body):
    # update data in mongo db
    data = json.loads(body)
    vid_id = update_vid_comment(data)
    
    LOG.info(f'Comments of video, {vid_id}, is updated')

channel.basic_consume(callback,
                      queue='comment',
                      no_ack=True)


LOG.info(' [*] Waiting for Job.')
channel.start_consuming()