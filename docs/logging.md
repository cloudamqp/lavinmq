
Logging is a crucial part of system observability, just like monitoring.
When developers and operators encounter issues or need to assess the system's state,
inspecting logs becomes essential.

Luckily, LavinMQ offers various logging features to facilitate this process.
To begin, let's look at where LavinMQ sends its log data to.

## LavinMQ log outputs

LavinMQ supports two log outputs:

- Standard output(Console)
- Log file

### Standard output(console)

By default LavinMQ will send its generated logs to the standard output. No configuration
needed for this - it happens out of the box.

### Log file

LavinMQ can also be configured to pipe its logs to a file. This can be done by
specifying the path to the log file in the `lavinmq.ini` file like so:
`log_file=/path/to/log/file`

In addition to the supported outputs above, logs can also be viewed/retrieved from
LavinMQ in a number of ways:

- **Managament interface:** You can download the last 1000 lines of logs from the management
  interface under the **Logs** tab.
- **HTTP API:** The HTTP API exposes two logs endpoints:
  - `/:your_server_url/api/logs` for retrieving the last 1000 lines of logs
  - `/:your_server_url/api/livelog` an [SSE](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events)
    endpoint that streams log data to a client over HTTP in real time

## LavinMQ log levels

Log levels provide a means to filter and adjust logging. They have a clear
hierarchy, with debug as the lowest severity and critical as the highest.

You can control logging verbosity on different levels by setting log levels
for categories and outputs. More verbose log levels include more log messages,
with debug being the most detailed and none being the least.

The log levels used by LavinMQ are as follows:

<div class="bg-[#181818] mt-6 border mb-8 border-[#414040] text-[#FAFAFA] overflow-hidden">
  <div style="overflow: auto;">
		<table class="divide-y divide-gray-300 conf-table">
			<thead>
				<tr>
					<th><span class="font-semibold">Log level</span></th>
					<th class="border-r border-[#414040]"><span class="font-semibold">Verbosity</span></th>
          <th><span class="font-semibold">Severity</span></th>
				</tr>
			</thead>
			<tbody>
        <tr>
          <td>debug</td>
          <td class="border-r border-[#414040]">most verbose</td>
          <td>lowest severity</td>
        </tr>
        <tr>
          <td>info</td>
          <td class="border-r border-[#414040]"></td>
          <td></td>
        </tr>
        <tr>
          <td>warn</td>
          <td class="border-r border-[#414040]"></td>
          <td></td>
        </tr>
        <tr>
          <td>error</td>
          <td class="border-r border-[#414040]"></td>
          <td></td>
        </tr>
        <tr>
          <td>fatal</td>
          <td class="border-r border-[#414040]">least verbose</td>
          <td>highest severity</td>
        </tr>
      </tbody>
    </table>
  </div>
</div>

The default debug level is `info`. However, this is configurable in the
`lavinmq.ini` file like so:

`log_level=warn`

## Wrap up

Logging is essential for system observability in LavinMQ. It offers easy
access to logs from standard output or log files. The flexibility of log
levels aids in effective troubleshooting and optimization.
