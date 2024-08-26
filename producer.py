from confluent_kafka import Producer, KafkaError
import json
import uuid
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
me = 'Marwa.Sallam'
# Kafka configuration
conf = {
    'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094',
    'client.id': me
}

# Create Producer instance
producer = Producer(conf)

def produce_msg(image_filename):
    """Produce a message to the Kafka topic with image metadata."""
    topic = me
    image_id = uuid.uuid4().hex
    value = {
        "id": image_id,
        "filename": image_filename
    }

    try:
        producer.produce(topic, key=None, value=json.dumps(value))
        producer.flush()
        logger.info(f"Produced message with image filename: {image_filename}")
    except KafkaError as e:
        logger.error(f"Failed to produce message: {e}")

if __name__ == "__main__":
    image_filename = input("Please enter the image filename: ")
    if image_filename:
        produce_msg(image_filename)
    else:
        logger.warning("No image filename provided.")
