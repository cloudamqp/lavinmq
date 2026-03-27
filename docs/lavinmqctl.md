
<!-- prettier-ignore-start -->

### What is a command line interface?

A command line interface lets you configure a program from your terminal. This has several benefits but one big advantage is that you can control your program from script files or from other programs.

### What benefits does lavinmqctl offer?

Being able to control LavinMQ from the command line can be very powerful, it allows you to do repetitive tasks like setting up exchanges and queues very simple. You can also use it to check the status of the broker.


### How should I use lavinmqctl?
Lavinmqctl can be used on your own computer or a server and you can use it against a local instance of lavinmq or a hosted one running on cloudamqp.com as it supports remote instances as well.

## Usage

`lavinmqctl` is a command line tool for managing an LavinMQ instance, it connects to the running instance over either unix socket or over HTTP. Default is using a unix socket, which therefore requires the instance to be running on the same machine. You can tell the tool to connect to a remote instance using HTTPS and using basic auth for authentication. When using this method the credentials to use are the same as you use when connecting an AMQP client so the same permissions is also used for `lavinmqctl`.

Example of listing queues in vhost cloudamqp

{% highlight shell %}
lavinmqctl -p cloudamqp list_queues
{% endhighlight %}

## Global options

Global options are available for all commands.

<div class="bg-[#181818] mt-6 border border-[#414040] text-[#FAFAFA] overflow-hidden">
  <div style="overflow: auto;">
		<table class="divide-y divide-gray-300 conf-table">
			<thead>
				<tr>
					<th><span class="font-semibold">Option</span></th>
					<th><span class="font-semibold">Description</span></th>
				</tr>
			</thead>
			<tbody>
        <tr>
          <td>-p vhost, --vhost=vhost</td>
          <td>Specify vhost</td>
        </tr>
        <tr>
          <td>-H host, --host=host</td>
          <td>Specify host, if skipped it will connect using the default unix socket, which require LavinMQ to be running on the same machine. <br> When connecting to a remote broker you should specify host a complete http uri like this: https://username:password@hostname.com/</td>
        </tr>
        <tr>
          <td>-q, --quiet</td>
          <td>Suppress informational messages</td>
        </tr>
        <tr>
          <td>-s, --silent</td>
          <td>Suppress informational messages and table formatting</td>
        </tr>
        <tr>
          <td>-v, --version</td>
          <td>Show version</td>
        </tr>
        <tr>
          <td>-h, --help</td>
          <td>Print all options and commands available, this can also be used for a command like “<code class="language-plaintext highlighter-rouge">lavinmqctl list_queue -h</code>” to show options for this command.</td>
        </tr>
        <tr>
          <td>-f format, --format=format</td>
          <td>Format output (text, json) default is text</td>
        </tr>
        <tr>
          <td>-n node, --node=node</td>
          <td>Does nothing, only used in CI for compatibility reasons.</td>
        </tr>
      </tbody>
    </table>
  </div>
</div>

## Commands

<div class="bg-[#181818] mt-6 border border-[#414040] text-[#FAFAFA] overflow-hidden">
  <div style="overflow: auto;">
		<table class="divide-y divide-gray-300 conf-table">
			<thead>
				<tr>
					<th><span class="font-semibold">Command</span></th>
					<th><span class="font-semibold">Description</span></th>
				</tr>
			</thead>
			<tbody>
        <tr>
          <td>status</td>
          <td>Display server status</td>
        </tr>
        <tr>
          <td>cluster_status</td>
          <td>Display cluster status</td>
        </tr>
        <tr>
          <td>add_user</td>
          <td>Creates a new user</td>
        </tr>
        <tr>
          <td>change_password</td>
          <td>Change the user password</td>
        </tr>
        <tr>
          <td>delete_user</td>
          <td>Delete a user</td>
        </tr>
        <tr>
          <td>list_users</td>
          <td>List user names and tags</td>
        </tr>
        <tr>
          <td>set_user_tags</td>
          <td>Sets user tags</td>
        </tr>
        <tr>
          <td>list_vhosts</td>
          <td>Lists virtual hosts</td>
        </tr>
        <tr>
          <td>add_vhost</td>
          <td>Creates a virtual host</td>
        </tr>
        <tr>
          <td>delete_vhost</td>
          <td>Deletes a virtual host</td>
        </tr>
        <tr>
          <td>reset_vhost</td>
          <td>Purges all messages in all queues in the vhost and close all consumers, optionally back up the data</td>
        </tr>
        <tr>
          <td>list_connections</td>
          <td>Lists AMQP 0.9.1 connections for the node</td>
        </tr>
        <tr>
          <td>close_connection</td>
          <td>Instructs the broker to close a connection by pid</td>
        </tr>
        <tr>
          <td>close_all_connections</td>
          <td>Instructs the broker to close all connections for the specified vhost or entire node</td>
        </tr>
        <tr>
          <td>list_queues</td>
          <td>Lists queues and their properties</td>
        </tr>
        <tr>
          <td>create_queue</td>
          <td>Create queue</td>
        </tr>
        <tr>
          <td>purge_queue</td>
          <td>Purges a queue (removes all messages in it)</td>
        </tr>
        <tr>
          <td>pause_queue</td>
          <td>Pause all consumers on a queue</td>
        </tr>
        <tr>
          <td>resume_queue</td>
          <td>Resume all consumers on a queue</td>
        </tr>
        <tr>
          <td>delete_queue</td>
          <td>Delete queue</td>
        </tr>
        <tr>
          <td>list_exchanges</td>
          <td>Lists exchanges</td>
        </tr>
        <tr>
          <td>create_exchange</td>
          <td>Create exchange</td>
        </tr>
        <tr>
          <td>delete_exchange</td>
          <td>Delete exchange</td>
        </tr>
        <tr>
          <td>set_policy</td>
          <td>Sets or updates a policy</td>
        </tr>
        <tr>
          <td>clear_policy</td>
          <td>Clears (removes) a policy</td>
        </tr>
        <tr>
          <td>list_policies</td>
          <td>Lists all policies in a virtual host</td>
        </tr>
        <tr>
          <td>definitions</td>
          <td>Generate definitions json from the data directory</td>
        </tr>
        <tr>
          <td>export_definitions</td>
          <td>Exports definitions in JSON</td>
        </tr>
        <tr>
          <td>import_definitions</td>
          <td>Import definitions in JSON</td>
        </tr>
        <tr>
          <td>hash_password</td>
          <td>Hash a password</td>
        </tr>
      </tbody>
    </table>
  </div>
</div>

<!-- prettier-ignore-end -->
