
The LavinMQ definitions file consist of users, queues, exchanges, bindings, virtual hosts, permissions, and parameters. These definitions can be exported to a file to import into another server or used for a schema backup.

### What is the definitions file?

The definition file contains information and definitions of all objects in LavinMQ, like queues, exchanges, bindings, users, virtual hosts, permissions, and parameters.

### Can the definitions file be exported and imported?

Yes, it can be exported to a JSON object, and imported as a JSON object.

### Why do I need to export the definitions file?

An exported definitions file can be used to restore the basic setup on a secondary LavinMQ server. It might be used when creating a backup or moving from one AMQP-server to another.

## How to export or import LavinMQ configuration

Definitions can be exported or imported as a JSON using any of the following methods:

- The [LavinMQ management interface](management-interface-overview.md)
- Using [lavinmqctl](lavinmqctl.md) commands export_definitions and import_defintions
- Calling the [api/definitions](https://docs.lavinmq.com/http-api.html#tag/definitions) API endpoint

Note: Be aware that the exported definitions contain hashed passwords.

### Using the LavinMQ management

The LavinMQ management overview tab contains two sections, namely Upload definitions and Export definitions.

<img alt="Upload and export definitions in LavinMQ management interface" src="img/docs/add-remove-defs-dm-2x.png" class="border border-[#414040]" />

#### Upload definitions

Upload the definitions by clicking Choose file, then select the JSON file containing the definitions, and click upload.

#### Export definitions

Definitions can be exported for a specific virtual host or for the entire LavinMQ server. Select All or a specific vhost from the drop-down and click _Download_. When definitions are exported for just one virtual host, some information will be excluded from the exported file.

### Using Lavinmqctl commands export_definitions and import_defintions

lavinmqctl allows for definitions to be exported in JSON format using the command `lavinmqctl export_definitions`.

Importing definitions is done using `lavinmqctl import_definitions <file>`

### Calling the api/definitions API endpoint

More information on the definitions export and import functionality can be found in the [LavinMQ API docs](https://docs.lavinmq.com/http-api.html#tag/definitions).
