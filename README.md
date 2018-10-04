# CS795 Assignment 1: S3KV

## Deployment

1. Run docker-compose to start local etcd service:

    ~~~
    docker-compose up -d
    ~~~

    Reset persistent storage

    ~~~
    docker-compose rm data
    docker-compose up -d
    ~~~

2. Install python dependencies:

    ~~~
    pip install python-etcd
    pip install boto3
    ~~~

3. Config aws:

    ~~~
    # in ~/.aws/credential
    [default]
    aws_access_key_id=<access_key>
    aws_secret_access_key=<access_secret>

    # in ~/.aws/config
    [default]
    region=us-east-1
    output=json
    ~~~

## Run

* Test Command

1. change "bucket_key" in driver.py
2. run:

    ~~~
    python driver.py
    ~~~

* Workloads:

1. change "bucket_key" in s3ky_slap.py
2. run:

    ~~~
    python s3ky_slap.py 5 wl_small.load 100
    python s3ky_slap.py 5 wl_small.run 100
    ~~~

## Reference

https://cs.gmu.edu/~yuecheng/teaching/cs795_fall18/assignments.html
