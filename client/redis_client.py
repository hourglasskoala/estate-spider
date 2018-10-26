import redis

redis_client = redis.StrictRedis(host='192.168.33.10',
                                  port='6379',
                                  db=15,
                                  max_connections=200)



