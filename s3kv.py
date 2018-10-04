import traceback
import logging
import re

import etcd
import boto3
import redis
import hashlib


class S3KV:
    etcd_client = None
    s3_client = None
    redis_client = None

    def __init__(self, etcd_host, etcd_port):
        self.etcd_client = etcd.Client(host=etcd_host, port=etcd_port)
        self.s3_client = boto3.client('s3')
        self.redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=0)

    def s3kv_init(self, etcd_host, etcd_port):
        self.etcd_client = etcd.Client(host=etcd_host, port=etcd_port)
        self.s3_client = boto3.client('s3')
        self.redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=0)

    def s3kv_create_bucket(self, bucket_name):
        # try:
        #     self.s3_client.create_bucket(  # create S3 bucket to S3 and get put ack
        #         Bucket = bucket_name
        #     )
        #     self.s3_client.put_bucket_versioning(
        #         Bucket = bucket_name,
        #         VersioningConfiguration={
        #             'Status': 'Enabled'
        #         }
        #     )
        # except Exception as e:
        #     logging.error(traceback.format_exc())

        lock = etcd.Lock(self.etcd_client, bucket_name)
        lock.acquire(blocking=True, lock_ttl=30)
        try:
            self.s3_client.create_bucket(
                Bucket=bucket_name
            )
            self.s3_client.put_bucket_versioning(
                Bucket=bucket_name,
                VersioningConfiguration={
                    'Status': 'Enabled'
                }
            )
        except Exception as e:
            logging.error(traceback.format_exc())
        lock.release()

    def s3kv_put(self, bucket_name, obj_key, obj_data):
        obj_key = re.sub(r'^/*(.+?)/*$', '\g<1>', obj_key)  # remove prepending and appending /
        lock_name = re.sub(r'^/*(.+?)/*$', '\g<1>/', bucket_name) + obj_key  # lock name <bucket_name>/<obj_key>
        etcd_name = re.sub(r'^/*(.+?)/*$', '/\g<1>/', bucket_name) + obj_key  # etcd name /<bucket_name>/<obj_key>
        radis_name = re.sub(r'^/*(.+?)/*$', '/\g<1>/', bucket_name) + obj_key  # redis name /<bucket_name>/<obj_key>

        lock = etcd.Lock(self.etcd_client, lock_name)

        # r = redis.Redis(host='localhost', port=6379, db=0)

        # Acquire the lock over the full path of data obj
        lock.acquire(blocking=True, lock_ttl=None)

        # redis.set(key, obj)  # Redis write path
        redis.set(obj_key, obj_data)

        # compute the hash value of the data obj
        m = hashlib.md5()
        m.update(obj_data)
        hash_obj_data = m.hexdigest()

        # original key concatenated with the hash value h
        version_key = obj_key + '/' + hash_obj_data

        # Write to S3 and get a s3 obj
        try:
            s3object = self.s3_client.put_object(
                Bucket=bucket_name,
                Key=version_key,
                Body=obj_data
            )
            self.etcd_client.write(etcd_name, hash_obj_data)  # Update etcd metadata server (MDS) with updated version id of s3 obj
        except Exception as e:
            logging.error(traceback.format_exc())

        lock.release()  # release the lock

    def s3kv_get(self, bucket_name, obj_key):
        obj_key = re.sub(r'^/*(.+?)/*$', '\g<1>', obj_key)  # remove prepending and appending /
        lock_name = re.sub(r'^/*(.+?)/*$', '\g<1>/', bucket_name) + obj_key  # lock name <bucket_name>/<obj_key>
        etcd_name = re.sub(r'^/*(.+?)/*$', '/\g<1>/', bucket_name) + obj_key  # etcd name /<bucket_name>/<obj_key>

        lock = etcd.Lock(self.etcd_client, lock_name)

        lock.acquire(blocking=True, lock_ttl=None)  # Acquire the lock over the full path of data obj

        # Redis read 1: fetch cached obj from Redis
        val_obj = redis.get(obj_key)

        # double check the hash value of fetched obj matches h
        if val_obj is not None:
            lock.release()  # release the lock
            return val_obj


        try:
            # For a Redis cache miss, read the latest hash value of the data obj / h is the hash value
            h = self.etcd_client.read(etcd_name).value
            while True:
                try:
                    # fetch specified version ID until found, also this could avoid interferences from outside.
                    response = self.s3_client.get_object(
                        Bucket=bucket_name,
                        Key=obj_key,
                        # VersionId = version_id
                    )  # keep fetching obj...

                    # until the fetched obj is not NULL
                    if response.get('Body').read() != None:
                        break
                except Exception as e:
                    response = None
                    # print("not found: " + version_id);
        except etcd.EtcdKeyNotFound:
            response = None
        except Exception as e:
            logging.error(traceback.format_exc())

        lock.release()  # release the lock

        # double check the hash value of fetched obj matches h
        obj_data = None
        if response:
            obj_data = response.get('Body').read()

        m = hashlib.md5()
        m.update(obj_data)
        hash_obj_data = m.hexdigest()

        if hash_obj_data == h:
            redis.set(obj_key, obj_data)
            return obj_data
        else:
            return None
