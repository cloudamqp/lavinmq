
A policy can be set to limit the number of messages stored in a queue. This can improve the server stability in the case where one of the server’s queues has an increasing number of enqueued messages. Otherwise, a single continuously growing queue may take down the entire server.

## Policy for Max Queue length

The maximum length allowed for a queue can be configured by setting a [policy](policies.md). Policies can be set from a terminal using [lavinmqctl](lavinmqctl.md) or by using the [HTTP API](https://docs.lavinmq.com/http-api.html) or [Web Management Interface](management-interface-overview.md).

The policy must be given a name and a pattern or exact match to determine which queues should have this policy. Additional options can be set including the number of messages or the total size of stored messages and how to handle messages published beyond the limit.

## Overflow

The Overflow setting determines what happens when messages beyond the allowed limit are sent to the queue. The options are to dequeue messages from the head of the queue (‘drop-head’) or to reject the message (‘reject-publish’). The default configuration for overflow is ‘drop-head’.

If the queue has a [dead-letter policy](policies.md) and overflow is set to ‘drop-head’, the dequeued messages will be sent to the dead letter queue. Otherwise, the dequeued messages will be deleted. In the case of ‘reject-publish’, new messages published to the queue that exceed the limit messages will be rejected.

## Max Queue Length

Max queue length is simply the number of messages allowed in the queue at any given time. Messages sent to a queue that has reached this limit will be handled according to the ‘Overflow’ policy explained above.

## Max Length Bytes

Alternatively, the queue can be limited by the size of the message data queue in bytes. When the data exceeds this amount, the handling described method by the Overflow setting is applied until the limit is no longer exceeded.

## Example Policies

Using the [lavinmqctl cli](lavinmqctl.md), we can set a max length queue policy for a queue. For this example, we want the policy to apply to a single queue (so we match the full queue name, ‘IT/Helpdesk/Requests’), to limit the number of messages in the queue to 1,000, and to reject messages published that exceed this limit. The policy name will be set to MessageCountLimit:

{% highlight shell %}
avalanchmqctl set_policy MessageCountLimit "IT/Helpdesk/Requests" \
 '{"max-length":1000,"overflow":"reject-publish"}'
{% endhighlight %}

For the second example, we want to apply the policy to all queues that start with the string ‘watchdog’, and we will limit the queue size to 1 MB, and drop messages at the head of the queue. The name of the policy will be set to WatchDogLimit. In this case, we will use the HTTP API:

{% highlight shell %}
curl -XPUT -u USERNAME:PWD --header "Content-Type: application/json"
--data ‘{"pattern":"^watchdog", "definition":{"max-length-bytes":1000, "overflow":"drop-head"}, "apply-to":"queues","priority":1}’ http://{AvMQ_HOSTNAME:15672}/api/policies/{VHOST}/WatchDogLimit
{% endhighlight %}

Note that here we have also set the priority of the policy as well as specified that the pattern is only applied to queues.

## Conclusion

Note that here we have also set the priority of the policy as well as specified that the pattern is only applied to queues.
