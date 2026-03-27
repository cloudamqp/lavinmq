
<!-- prettier-ignore-start -->

Even though publisher confirms and consumer acknowledgements offer some
level of message persistence guarantee in LavinMQ, that guarante is not 100%.
And beyond just reliable message persistence, publisher confirms are limited
in some ways.

This is true because:

- When using publisher confirms in LavinMQ, the server acknowledges the producer 
upon receiving the message and writing it to a file, but in a volatile way. So there
is no guarantee that messages would survive a server crash or disk error.

- In LavinMQ, consumer acknowledgements confirm successful message processing. 
Once acknowledged, the message gets removed from the queue. However, if the 
LavinMQ server crashes before receiving the acknowledgment, the processed message 
remains in the queue and may be redelivered to the consumer - resulting in duplicate 
processing.

- Traditional message publishing in LavinMQ lacks a roll-back mechanism after 
publishing. This limitation affects scenarios where you might need to trigger 
a database update(for example) after message publishing and roll back the message 
published if the update fails.

Transactions in LavinMQ help mitigate the above listed challenges.

## What are transactions?
The concept of a transaction in LavinMQ is quite similar to a transaction in relational
databases.

Transactions in LavinMQ allow a producer to publish one or more messages in transaction 
mode, then commit these messages to the server or roll back all the published messages. 
Similarly, a consumer can send one or more acknowledgements in transaction mode then commit
these acknowledements or roll back.

In fact, you can even take this a step further and combine AMQP actions with other
things. For example, you can publish one or more messages, trigger a database update
or a network request to some API and only commit the publishes if the database
update or the network request passes. If the request or database update fails then you
roll-back.

In transaction mode, published messages are temporarily sent to the server 
but not saved until you perform a commit. Upon committing, the LavinMQ server 
writes the messages to disk and returns a `commit-ok` to the client. Alternatively, 
if you no longer wish to save the published messages on the server, you can opt 
for a roll-back instead of a commit.

## When can I use transactions?
You can use transactions in LavinMQ in the following scenarios:

- You can use transactions in scenarios where you want the highest possible
persistence guarantee.

- You can use transactions when publishing messages and making calls to 
an external service to ensure that the messages are saved on the server only if 
the external service call succeeds.

## Usage
To demonstrate working with transactions in LavinMQ we will consider
a use case.

Imagine a scenario where you want to publish some messages then trigger
a database update. You only want the messages to be saved on the server
only when the database update goes through.

For the purpose of this tutorial we will not be doing an actual database
update, we will just simulate it.

To begin, create your AMQP connection and channel as you normally would.
We are using Pika, the Python client.

{% highlight python %}
{% raw %}
# transactions_producer.py

import pika, os, random
from dotenv import load_dotenv

load_dotenv()

# Access the CLOUDAMQP_URL environment variable and parse it (fallback to localhost)
url = os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@localhost:5672/%2f')

# Create a connection
params = pika.URLParameters(url)
connection = pika.BlockingConnection(params)
print("[✅] Connection over channel established")

channel = connection.channel() # start a channel

{% endraw %}
{% endhighlight %}

Next, we need to set the channel to use transaction mode with:
`channel.tx_select()`

Next, you declare your queue and publish messages as you normally would:

{% highlight python %}
{% raw %}
channel.queue_declare(queue="transactions") # Declare a queue
def send_to_queue(channel, routing_key, body):
  channel.basic_publish(
        exchange='',
        routing_key=routing_key,
        body=body
  )
  print(f"[📥] Message sent to queue #{body}")

# Publish messages
send_to_queue(
    channel=channel, routing_key="transactions", body="Hello World"
)
send_to_queue(
    channel=channel, routing_key="transactions", body="Hello World"
)
send_to_queue(
    channel=channel, routing_key="transactions", body="Hello World"
)
{% endraw %}
{% endhighlight %}

Next, to simulate an update to a database, we will define a function that
randonmly generates numbers between 0 - 10. If the generated number is even,
then we say the database update went through and we commit the transaction.
Otherwise we say the database update failed and we roll back the transaction.

{% highlight python %}
{% raw %}
def db_update_successful():
    num = random.randrange(0, 10)
    
    if num % 2 == 0:
        print(f"[✅] Number: {num} - Database update PASSED, commiting the transaction...")
        return True
    
    print(f"[❎] Number: {num} -  Database update FAILED, rolling back the transaction...")
    return False
  
# Commit or roll back published messages based on the outcome of database update
if db_update_successful():
   channel.tx_commit()
else:
   channel.tx_rollback

try:
  connection.close()
  print("[❎] Connection closed")
except Exception as e:
  print(f"Error: #{e}")
{% endraw %}
{% endhighlight %}

### Putting everything together
{% highlight python %}
{% raw %}
# transactions_producer.py

import pika, os, random
from dotenv import load_dotenv

load_dotenv()

# Access the CLOUDAMQP_URL environment variable and parse it (fallback to localhost)
url = os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@localhost:5672/%2f')

# Create a connection
params = pika.URLParameters(url)
connection = pika.BlockingConnection(params)
print("[✅] Connection over channel established")

channel = connection.channel() # start a channel
channel.tx_select() # start channel in transactions mode

channel.queue_declare(queue="transactions") # Declare a queue
def send_to_queue(channel, routing_key, body):
  channel.basic_publish(
        exchange='',
        routing_key=routing_key,
        body=body
  )
  print(f"[📥] Message sent to queue #{body}")

# Publish messages
send_to_queue(
    channel=channel, routing_key="transactions", body="Hello World"
)
send_to_queue(
    channel=channel, routing_key="transactions", body="Hello World"
)
send_to_queue(
    channel=channel, routing_key="transactions", body="Hello World"
)

def db_update_successful():
    num = random.randrange(0, 10)
    
    if num % 2 == 0:
        print(f"[✅] Number: {num} - Database update PASSED, commiting the transaction...")
        return True
    
    print(f"[❎] Number: {num} -  Database update FAILED, rolling back the transaction...")
    return False
  
# Commit or roll back published messages based on outcome of data
if db_update_successful():
   channel.tx_commit()
else:
   channel.tx_rollback

try:
  connection.close()
  print("[❎] Connection closed")
except Exception as e:
  print(f"Error: #{e}")
  
{% endraw %}
{% endhighlight %}

If you run the script above, published messages will only be saved
on the server if the generated number is even - you can monitor this
from the admin view.

## Wrap up
In conclusion, transactions in LavinMQ provide a powerful and flexible 
solution for ensuring reliable message processing and data integrity. 
Generally, transactions address challenges such as potential message loss 
during server crashes, duplicate message processing, and the need for roll-back 
mechanisms after publishing or sending acknowledgements to the server.

<!-- prettier-ignore-end -->
