import sys

import pika

def send():
    try:
        # Create connection with explicit parameters
        credentials = pika.PlainCredentials('guest', 'guest')  # default credentials
        parameters = pika.ConnectionParameters(
            host='localhost',
            port=5672,  # explicit port
            credentials=credentials,
            connection_attempts=3,  # retry connection up to 3 times
            retry_delay=2  # wait 2 seconds between retries
        )

        print("Attempting to connect to RabbitMQ...")
        connection = pika.BlockingConnection(parameters)

        print("Creating channel...")
        channel = connection.channel()

        print("Declaring queue 'hello'...")
        # Make queue durable and ensure it exists
        channel.queue_declare(queue='task_queue', durable=True)

        message = ''.join(sys.argv[1:]) or "Hello World!"
        print(f"Sending message: {message}")

        # Send message with delivery confirmation
        channel.basic_publish(
            exchange='',
            routing_key='hello',
            body=message.encode(),  # properly encode the message
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,  # make message persistent
            )
        )

        # Close the connection properly
        connection.close()
        print("Connection closed successfully")

    except pika.exceptions.AMQPConnectionError as e:
        print(f"Could not connect to RabbitMQ: {e}")
    except pika.exceptions.ChannelError as e:
        print(f"Channel error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    send()