
<div class="accordion">
  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect1" id="accordion1">
        What is a vhost?
      </button>
    </h3>
    <div id="sect1" class="accordion-content" role="region" aria-labelledby="accordion1">
        <p>
          A vhost in LavinMQ is a logical container that groups connections, exchanges, queues, bindings, user permissions, and other resources within LavinMQ. Each vhost is isolated, allowing management and control of resources separately within the server.
        </p>
    </div>
  </div>

  <div class="accordion-item">
    <h3>
      <button class="accordion-header" aria-expanded="false" aria-controls="sect2" id="accordion2">
        What benefits does a vhost offer?
      </button>
    </h3>
    <div id="sect2" class="accordion-content" role="region" aria-labelledby="accordion2">
      <p>
        A vhost in LavinMQ helps separate different applications on the same server. Multiple development instances can exist on the same LavinMQ server.
      </p>
    </div>
  </div>
</div>

When a client establishes a connection to the LavinMQ server, it specifies the vhost in which it operates, for example `amqp://myuser:mypassword@localhost:5672/myvhost`

<img src="img/docs/lavinmq-vhost-example.png" class="border border-[#414040]" alt="LavinMQ vhost" />

Resources such as exchanges and queues are named entities inside the vhost container, making each vhost function as a LavinMQ mini-server. When configuring LavinMQ, at least one vhost is required; by default, it is named “/”.

Vhosts do not share exchanges or queues, and users, policies, and other settings are unique to each vhost.

### Using vhosts in LavinMQ

To create a vhost in LavinMQ, use the management interface or the HTTP API. A newly created vhost always has a default set of exchanges, no other entities, and no user permissions.

### LavinMQ HTTP API

1. Create a vhost:

   ```
   curl -X PUT http://username:password@localhost:15672/api/vhosts/vhost_name
   ```

   Replace `vhost_name` with the desired virtual host name.

2. Grant a user access to the host:

   ```
   curl -X PUT http://username:password@localhost:15672/api/permissions/vhost_name/username \
   -d '{"configure":".*", "write":".*", "read":".*"}' \
   -H "Content-Type: application/json"
   ```

   Replace `vhost_name` with the vhost name, `username` with the user's name, and modify the regex pattern (`".*"`) to restrict permissions.

### LavinMQ Management Interface

1. Log in to the LavinMQ management interface.
2. Navigate to the Virtual hosts section.
3. Enter a vhost name and adjust permissions.
4. Click _Add virtual host_ to create it.

<img class="border border-[#414040]" alt="Management Interface vhost" src="img/docs/vhost-dm.png" />
