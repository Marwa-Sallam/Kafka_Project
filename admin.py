from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
me = 'Marwa.Sallam'
# Kafka configuration
conf = {
    'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094',
}

# Create AdminClient instance
admin_client = AdminClient(conf)

# Topic configuration
topic_name = me
num_partitions = 1
replication_factor = 1

def create_topic(topic_name):
    """Create a new Kafka topic with the specified name."""
    try:
        # Create a NewTopic instance
        topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
        
        # Request topic creation
        fs = admin_client.create_topics([topic])
        
        # Handle the result of topic creation
        for topic, f in fs.items():
            try:
                f.result()  # Wait for the topic creation to complete
                logger.info(f"Topic '{topic}' created successfully")
            except KafkaException as e:
                logger.error(f"Failed to create topic '{topic}': {e}")
    except KafkaException as e:
        logger.error(f"Kafka exception occurred: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    create_topic(topic_name)
