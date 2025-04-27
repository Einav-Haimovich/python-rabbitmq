import time

import pika
import sys

def receive():
    try:
        # Create connection with explicit parameters
        credentials = pika.PlainCredentials('guest', 'guest')
        parameters = pika.ConnectionParameters(
            host='localhost',
            port=5672,
            credentials=credentials,
            connection_attempts=3,
            retry_delay=2
        )

        print("Attempting to connect to RabbitMQ...")
        connection = pika.BlockingConnection(parameters)

        print("Creating channel...")
        channel = connection.channel()

        print("Declaring queue 'hello'...")
        channel.queue_declare(queue='task_queue', durable=True)

        def callback(ch, method, properties, body):
            print(f" [x] Received {body.decode()}")
            time.sleep(body.count(b'.'))

            print(' [x] Done')
            # Acknowledge the message
            ch.basic_ack(delivery_tag=method.delivery_tag)

        # Fair dispatch
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue='task_queue',on_message_callback=callback)

        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()

    except KeyboardInterrupt:
        print("Interrupted by user, shutting down...")
        try:
            connection.close()
            sys.exit(0)
        except Exception:
            pass
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == '__main__':
    receive()