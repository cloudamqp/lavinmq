import logging

from amqpstorm import UriConnection


logging.basicConfig(level=logging.DEBUG)


def publisher():
    with UriConnection('amqp://guest:guest@localhost:5672/default') as connection:
        with connection.channel() as channel:
            # Declare the Queue, 'simple_queue'.
            channel.queue.declare('simple_queue')

            # Message Properties.
            properties = {
                'content_type': 'text/plain',
                'headers': {'key': 'value'}
            }

            # Publish the message to a queue called, 'simple_queue'.
            channel.basic.publish(body='Hello World!',
                                  routing_key='simple_queue',
                                  properties=properties)


if __name__ == '__main__':
    publisher()
