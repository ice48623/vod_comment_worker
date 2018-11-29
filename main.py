#!/usr/bin/env python3
from pymongo import MongoClient
import os
import pika
import json
import logging
from rabbit import Rabbit


MONGO_URL = os.getenv('MONGO_URL', 'localhost')
MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
MONGO_DB = os.getenv('MONGO_DB', 'my_db')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'my_collection')
mongoClient = MongoClient(f'mongodb://{MONGO_URL}', MONGO_PORT)
db = mongoClient[MONGO_DB]
collection = db[MONGO_COLLECTION]

LOG = logging
LOG.getLogger('pika').setLevel(LOG.INFO)
LOG.basicConfig(
    level=LOG.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def update_vid_comment(data):
    video_id = data.get('video_id')
    comment = data.get('comment')
    uid = data.get('uid')

    db_comment = {
        'comment': comment,
        'uid': uid
    }

    collection.find_one_and_update(
        {'video_id': video_id},
        {'$push': {'comments': db_comment}}
    )

    return video_id

def callback(ch, method, properties, body):
    # update data in mongo db
    data = json.loads(body)
    vid_id = update_vid_comment(data)
    
    LOG.info(f'Comments of video, {vid_id}, is updated')

if __name__ == '__main__':
    rabbit = Rabbit('comment')
    rabbit.consume(callback)

    LOG.info(' [*] Waiting for Job.')
    rabbit.start_consuming()