import base_client_01
import time

redis_client = base_client_01.redis_client

def produce_messages_to_queue():
    queue_name = "produced_queue_01"
    message = {
        "ID": 1,
        "name": "lukasz",
        "surname": "dobek"
    }
    for i in range(100):
        base_client_01.send_dictionary_to_queue(queue_name, message)
        message["ID"]+=1
        time.sleep(1)

produce_messages_to_queue()
