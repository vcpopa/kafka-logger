from kafka import KafkaConsumer
import json
from colored import fg, bg, attr

# Kafka broker host and port
kafka_host = 'desktop-plsjgtd-docker-desktop'
kafka_port = 29092

# Create a Kafka consumer
consumer = KafkaConsumer('test_topic',
                         bootstrap_servers=f'{kafka_host}:{kafka_port}',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Function to print colored message

def print_colored_message(message):
    org = message['org']
    pipeline = message['pipeline']
    status = message['status']
    
    if status == 1:
        text_color = fg('green')
    elif status == 0:
        text_color = fg('red')
    elif status == 2:
        text_color = fg('yellow')
    else:
        text_color = fg('default')  # Use default color for unknown status
    
    org_color = fg('blue')
    pipeline_color = fg('blue')

    formatted_message = f"{org_color}{org} - {pipeline_color}{pipeline}{attr(0)} - {text_color}{message['message']}{attr(0)}"
    print(formatted_message)

# Continuously listen for messages
print("Listening....")
try:
    for message in consumer:
        print_colored_message(message.value)
except Exception as e:
    print(f"Error: {e}")
finally:
    consumer.close()
