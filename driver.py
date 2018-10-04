from s3kv import S3KV

myapp = S3KV('127.0.0.1', 2379)

bucket_key = 'demo.cs795.ao'

# myapp.s3kv_create_bucket(bucket_key);
myapp.s3kv_get(bucket_key, 'key0001')
myapp.s3kv_put(bucket_key, 'key0001', 'value0001')
myapp.s3kv_get(bucket_key, 'key0001')
myapp.s3kv_put(bucket_key, 'key0001', 'value0002')
myapp.s3kv_get(bucket_key, 'key0001')
