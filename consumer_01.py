import base_client_01
import time
import json

def consume_messages_from_queue():
    queue_name = "produced_queue_01"
    while(True):
        print("Pulled:")
        print(json.loads(base_client_01.pull_dictionary_from_queue(queue_name)))
        time.sleep(5)

consume_messages_from_queue()