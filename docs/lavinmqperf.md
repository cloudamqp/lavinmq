
<!-- prettier-ignore-start -->

`lavinmqperf` is a command-line tool designed to help you test the performance of your LavinMQ service. It allows you to spin up clients on your server and measure the performance of LavinMQ as these clients interact with it in different ways.`lavinmqperf` provides real-time updates and a summary report of the results at the end of each test.

## Usage

You can run `lavinmqperf` with the command line input

{% highlight shell %}
{% raw %}
bin/lavinmqperf [protocol] [throughput | bind-churn | queue-churn | connection-churn | connection-count | queue-count] [arguments]
{% endraw %}
{% endhighlight %}

`[protocol]` can be either `amqp` or `mqtt`, depending on which protocol you want to test. The `[throughput | bind-churn | queue-churn | connection-churn | connection-count | queue-count]` specifies the type of test you want to run. The `[arguments]` are optional parameters that allow you to customize the test.

If you don't specify any protocol or arguments, `lavinmqperf` will run the tests with default AMQP settings. Please see the Parameters section below for more information.

## Example

Here is a simple example on how to run a throughput test with `lavinmqperf` on your local setup.

First, make sure LavinMQ is running on your machine

{% highlight shell %}
{% raw %}
crystal run src/lavinmq.cr -- -D /tmp/amqp
{% endraw %}
{% endhighlight %}

Go to the LavinMQ repository and run

{% highlight shell %}
{% raw %}
bin/lavinmqperf throughput -x 1 -y 2
{% endraw %}
{% endhighlight %}

This command runs a throughput test with `lavinmqperf`, using one publisher and two consumers.

## AMQP Features

`lavinmqperf` supports various performance metrics to help you evaluate the performance of your LavinMQ service. The available metrics for the AMQP protocol include:

#### Throughput

Measures the number of messages per second that LavinMQ can sustain to publish and consume.

#### Bind-churn

Measures the maximum rate at which LavinMQ can bind to both a durable and a non-durable queue.

#### Queue-churn

Measures the maximum rate at which LavinMQ can create and delete a queue.

#### Connection-churn

Measures the maximum rate at which clients can connect and disconnect.

#### Connection-count

Measures the number of connections that LavinMQ can sustain, as well as the amount of memory used by those connections.

#### Queue-count

Measures the number of queues that LavinMQ can sustain.

### Parameters

`lavinmqperf` provides several parameters that allow you to customize the tests to your AMQP needs. Here is a table that explains the available parameters and how to set them:

<div class="bg-[#181818] mt-6 border mb-8 border-[#414040] text-[#FAFAFA] overflow-hidden">
  <div style="overflow: auto;">
		<table class="divide-y divide-gray-300 conf-table">
			<thead>
				<tr>
					<th><span class="font-semibold">Option / Flag</span></th>
					<th class="border-r border-[#414040]"><span class="font-semibold">Description</span></th>
          <th><span class="font-semibold">Default Value</span></th>
				</tr>
			</thead>
			<tbody>
        <tr>
          <td>-x, --publishers=number</td>
          <td class="border-r border-[#414040]">Number of publishers</td>
          <td><span class="text-[#FAFAFA]">1</span></td>
        </tr>
        <tr>
          <td>-y, --consumers=number</td>
          <td class="border-r border-[#414040]">Number of consumers</td>
          <td><span class="text-[#FAFAFA]">1</span></td>
        </tr>
        <tr>
          <td>-s, --size=bytes</td>
          <td class="border-r border-[#414040]">Size of each message</td>
          <td><span class="text-[#FAFAFA]">16 bytes</span></td>
        </tr>
        <tr>
          <td>-V, --verify</td>
          <td class="border-r border-[#414040]">Verify the message body</td>
          <td><span class="text-[#FAFAFA]">false</span></td>
        </tr>
        <tr>
          <td>-a, --ack=messages</td>
          <td class="border-r border-[#414040]">Ack after X consumed messages</td>
          <td><span class="text-[#FAFAFA]">0</span></td>
        </tr>
        <tr>
          <td>-c, --confirm=max-unconfirmed</td>
          <td class="border-r border-[#414040]">Confirm publishes every X messages</td>
          <td>&nbsp;</td>
        </tr>
        <tr>
          <td>-t, --transaction=messages</td>
          <td class="border-r border-[#414040]">Publish messages in transactions</td>
          <td>&nbsp;</td>
        </tr>
        <tr>
          <td>-T, --transaction=acknowledgements</td>
          <td class="border-r border-[#414040]">Ack messages in transactions</td>
          <td>&nbsp;</td>
        </tr>
        <tr>
          <td>-u, --queue=name</td>
          <td class="border-r border-[#414040]">Queue name</td>
          <td><span class="text-[#FAFAFA]">perf-test</span></td>
        </tr>
        <tr>
          <td>-k, --routing-key=name</td>
          <td class="border-r border-[#414040]">Routing key</td>
          <td><span class="text-[#FAFAFA]">Same as queue name</span></td>
        </tr>
        <tr>
          <td>-e, --exchange=name</td>
          <td class="border-r border-[#414040]">Exchange to publish to</td>
          <td><span class="text-[#FAFAFA]">””</span></td>
        </tr>
        <tr>
          <td>-r, --rate=number</td>
          <td class="border-r border-[#414040]">Max publish rate</td>
          <td><span class="text-[#FAFAFA]">0</span></td>
        </tr>
        <tr>
          <td>-R, --consumer-rate=number</td>
          <td class="border-r border-[#414040]">Max consume rate</td>
          <td><span class="text-[#FAFAFA]">0</span></td>
        </tr>
        <tr>
          <td>-p, --persistent</td>
          <td class="border-r border-[#414040]">Persistent messages</td>
          <td><span class="text-[#FAFAFA]">false</span></td>
        </tr>
        <tr>
          <td>-P, --prefetch=number</td>
          <td class="border-r border-[#414040]">Number of messages to prefetch</td>
          <td><span class="text-[#FAFAFA]">0 (unlimited)</span></td>
        </tr>
        <tr>
          <td>-g, --poll</td>
          <td class="border-r border-[#414040]">Poll with basic_get instead of consuming</td>
          <td><span class="text-[#FAFAFA]">false</span></td>
        </tr>
        <tr>
          <td>-j, --json</td>
          <td class="border-r border-[#414040]">Output result as JSON</td>
          <td><span class="text-[#FAFAFA]">false</span></td>
        </tr>
        <tr>
          <td>-z, --time=seconds</td>
          <td class="border-r border-[#414040]">Only run for X seconds</td>
          <td><span class="text-[#FAFAFA]">Unlimited</span></td>
        </tr>
        <tr>
          <td>-q, --quiet</td>
          <td class="border-r border-[#414040]">Quiet, only print the summary</td>
          <td><span class="text-[#FAFAFA]">false</span></td>
        </tr>
        <tr>
          <td>-C, --pmessages=messages</td>
          <td class="border-r border-[#414040]">Publish max X number of messages</td>
          <td><span class="text-[#FAFAFA]">Unlimited</span></td>
        </tr>
        <tr>
          <td>-D, --cmessages=messages</td>
          <td class="border-r border-[#414040]">Consume max X number of messages</td>
          <td><span class="text-[#FAFAFA]">Unlimited</span></td>
        </tr>
        <tr>
          <td>--queue-args=JSON</td>
          <td class="border-r border-[#414040]">Queue arguments as a JSON string</td>
          <td>&nbsp;</td>
        </tr>
        <tr>
          <td>--consumer-args=JSON</td>
          <td class="border-r border-[#414040]">Consumer arguments as a JSON string</td>
          <td>&nbsp;</td>
        </tr>
        <tr>
          <td>--properties=JSON</td>
          <td class="border-r border-[#414040]">Properties added to published messages</td>
          <td>&nbsp;</td>
        </tr>
        <tr>
          <td>--random-bodies</td>
          <td class="border-r border-[#414040]">Each message body is random</td>
          <td><span class="text-[#FAFAFA]">false</span></td>
        </tr>
      </tbody>
    </table>
  </div>
</div>

## MQTT Features

#### Throughput

Measures the number of messages per second that LavinMQ can sustain to publish and consume.

### Parameters

`lavinmqperf` provides several parameters that allow you to customize the tests to your MQTT needs. Here is a table that explains the available parameters and how to set them:

<div class="bg-[#181818] mt-6 border mb-8 border-[#414040] text-[#FAFAFA] overflow-hidden">
  <div style="overflow: auto;">
		<table class="divide-y divide-gray-300 conf-table">
			<thead>
				<tr>
					<th><span class="font-semibold">Option / Flag</span></th>
					<th class="border-r border-[#414040]"><span class="font-semibold">Description</span></th>
          <th><span class="font-semibold">Default Value</span></th>
				</tr>
			</thead>
			<tbody>
        <tr>
          <td>-x, --publishers=number</td>
          <td class="border-r border-[#414040]">Number of publishers</td>
          <td><span class="text-[#FAFAFA]">1</span></td>
        </tr>
        <tr>
          <td>-y, --consumers=number</td>
          <td class="border-r border-[#414040]">Number of consumers</td>
          <td><span class="text-[#FAFAFA]">1</span></td>
        </tr>
        <tr>
          <td>-s, --size=bytes</td>
          <td class="border-r border-[#414040]">Size of each message</td>
          <td><span class="text-[#FAFAFA]">16 bytes</span></td>
        </tr>
        <tr>
          <td>-V, --verify</td>
          <td class="border-r border-[#414040]">Verify the message body</td>
          <td><span class="text-[#FAFAFA]">false</span></td>
        </tr>
        <tr>
          <td>-q, --qos=level</td>
          <td class="border-r border-[#414040]">QoS level (0 or 1)</td>
          <td><span class="text-[#FAFAFA]">0</span></td>
        </tr>
        <tr>
          <td>-t, --topic=name</td>
          <td class="border-r border-[#414040]">Topic name</td>
          <td><span class="text-[#FAFAFA]">perf-test</span></td>
        </tr>
        <tr>
          <td>-r, --rate=number</td>
          <td class="border-r border-[#414040]">Max publish rate</td>
          <td><span class="text-[#FAFAFA]">0</span></td>
        </tr>
        <tr>
          <td>-R, --consumer-rate=number</td>
          <td class="border-r border-[#414040]">Max consume rate</td>
          <td><span class="text-[#FAFAFA]">0</span></td>
        </tr>
        <tr>
          <td>-j, --json</td>
          <td class="border-r border-[#414040]">Output result as JSON</td>
          <td><span class="text-[#FAFAFA]">false</span></td>
        </tr>
        <tr>
          <td>-z, --time=seconds</td>
          <td class="border-r border-[#414040]">Only run for X seconds</td>
          <td><span class="text-[#FAFAFA]">Unlimited</span></td>
        </tr>
        <tr>
          <td>-q, --quiet</td>
          <td class="border-r border-[#414040]">Quiet, only print the summary</td>
          <td><span class="text-[#FAFAFA]">false</span></td>
        </tr>
        <tr>
          <td>-C, --pmessages=messages</td>
          <td class="border-r border-[#414040]">Publish max X number of messages</td>
          <td><span class="text-[#FAFAFA]">Unlimited</span></td>
        </tr>
        <tr>
          <td>-D, --cmessages=messages</td>
          <td class="border-r border-[#414040]">Consume max X number of messages</td>
          <td><span class="text-[#FAFAFA]">Unlimited</span></td>
        </tr>
        <tr>
          <td>--random-bodies</td>
          <td class="border-r border-[#414040]">Each message body is random</td>
          <td><span class="text-[#FAFAFA]">false</span></td>
        </tr>
        <tr>
          <td>--retain</td>
          <td class="border-r border-[#414040]">Set retain flag on published messages</td>
          <td><span class="text-[#FAFAFA]">false</span></td>
        </tr>
        <tr>
          <td>--clean-session</td>
          <td class="border-r border-[#414040]">Use clean session</td>
          <td><span class="text-[#FAFAFA]">false</span></td>
        </tr>
        <tr>
          <td>-u, --uri=uri</td>
          <td class="border-r border-[#414040]">MQTT broker URI</td>
          <td><span class="text-[#FAFAFA]">mqtt://localhost:1883</span></td>
        </tr>
      </tbody>
    </table>
  </div>
</div>

<!-- prettier-ignore-end -->
