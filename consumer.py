import tensorflow as tf
import tensorflow_hub as hub
import numpy as np
import cv2
import requests
import json
import logging
import os
from confluent_kafka import Consumer, KafkaException, KafkaError
me = 'Marwa.Sallam'
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka configuration
conf = {
    'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094',
    'group.id': me,
    'auto.offset.reset': 'smallest',
    'enable.auto.commit': True
}

# Create Consumer instance
consumer = Consumer(conf)
consumer.subscribe(['me'])

# Load TensorFlow Hub model
model_url = 'https://tfhub.dev/google/faster_rcnn/openimages_v4/inception_resnet_v2/1'
detector = hub.load(model_url)

def load_image(image_path):
    """Load and preprocess the image."""
    image = cv2.imread(image_path)
    image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
    image = cv2.resize(image, (640, 640))  # Resize image to the size expected by the model
    image = image.astype(np.float32)
    image = np.expand_dims(image, axis=0)  # Add batch dimension
    return image

def detect_object(image_path):
    """Perform object detection using TensorFlow Hub model."""
    image = load_image(image_path)
    result = detector(image)
    boxes = result['detection_boxes'].numpy()
    classes = result['detection_classes'].numpy()
    scores = result['detection_scores'].numpy()

    # Example processing: return the first detected object with a score > 0.5
    for i in range(len(scores[0])):
        if scores[0][i] > 0.5:
            detected_class = int(classes[0][i])
            labels = ['Car', 'House', 'Bike']  # Adjust based on actual class labels
            return labels[detected_class]  # Return the detected object label

    return 'unknown'

def process_msg(msg):
    """Process the incoming Kafka message."""
    try:
        # Print raw message for debugging
        raw_value = msg.value().decode('utf-8')
        logger.info(f"Raw message: {raw_value}")

        # Check if the message is empty
        if not raw_value:
            logger.warning("Received empty message.")
            return

        msg_value = json.loads(raw_value)
        image_id = msg_value.get('id')
        filename = msg_value.get('filename')
        if not image_id or not filename:
            logger.warning("No image ID or filename found in message.")
            return

        # Download the image file
        image_url = f'http://127.0.0.1:5000/images/{filename}'
        response = requests.get(image_url, stream=True)
        if response.status_code == 200:
            image_path = f'D:/Kafka/Project/images/{filename}'
            with open(image_path, 'wb') as f:
                f.write(response.content)
            detected_object = detect_object(image_path)
            logger.info(f"Detected object: {detected_object}")

            response = requests.put(f'http://127.0.0.1:5000/object/{image_id}', json={"object": detected_object})
            if response.status_code == 200:
                logger.info(f"Successfully updated object for image {image_id}")
            else:
                logger.error(f"Failed to update object for image {image_id}, status code: {response.status_code}")
        else:
            logger.error(f"Failed to download image, status code: {response.status_code}")

    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON: {e}")
    except requests.RequestException as e:
        logger.error(f"Request error: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logger.warning(f'%% {msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}')
            else:
                raise KafkaException(msg.error())
        else:
            process_msg(msg)
            consumer.commit(asynchronous=False)
finally:
    consumer.close()
