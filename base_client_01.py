import redis
import json

redis_client = redis.StrictRedis(host='localhost', port=6381, db=0)

def clear_current_db():
    redis_client.flushdb()

def get_queue_items(queue_name, start, stop):
    # https://redis.io/commands/LRANGE
    return redis_client.lrange(queue_name, start, stop)

def print_whole_queue(queue_name):
    first_item = 0
    last_item = -1
    whole_queue = get_queue_items(queue_name, first_item, last_item)
    for value_from_queue in whole_queue:
        print(json.loads(value_from_queue)) # mogloby byc samo print(value_from_queue) ale wtedy brzydko sie formatuje (dodaje b i [])

def send_dictionary_to_queue(queue_name, dictionary):
    redis_client.rpush(queue_name, json.dumps(dictionary))

def pull_queue_from_db(queue_name):
    first_item = 0
    last_item = -1
    queue = get_queue_items(queue_name, first_item, last_item)
    length_of_queue = len(queue)
    redis_client.ltrim(queue_name, length_of_queue, last_item)
    return queue

def delete_queue_from_db(queue_name):
    deleted_queue = pull_queue_from_db(queue_name)

def pull_dictionary_from_queue(queue_name):
    # https://redis.io/commands/lpop
    return redis_client.lpop(queue_name)

if __name__ == '__main__':
    queue_name = "queue_01"
    sample_dictionary = {
        "a":1,
        "b":2,
        "c":3
    }
    send_dictionary_to_queue(queue_name, sample_dictionary)

    queue_name_temp = "queue_02"
    send_dictionary_to_queue(queue_name_temp, sample_dictionary)




