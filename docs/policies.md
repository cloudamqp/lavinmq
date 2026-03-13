
In order to ensure uniformly configured queues and exchanges, LavinMQ (AMQP) includes the ability to define Policies and [Arguments](/documentation/arguments-and-properties). The specifications of the AMQP protocol (0.9.1) enables support for various features, called Arguments. Depending on which argument you implement, changes can be made to their settings after the queue declaration. Arguments define certain configurations, such as message and queue TTL, different consumer priorities, and queue length limit. Policies make it possible to configure arguments for one or many queues and exchanges at once, and the queues/exchanges will all be updated when the policy definition is updated. Policies can be changed at any time, and changes will affect all matching queues and exchanges.

![LavinMQ Arguments Policy](/img/docs/lavinmq-arguments-policy.jpg)

### What is a policy?

Policies can be advantageously used to apply queue or exchange arguments to more than one created queue/exchange.

### What benefits do policies have?

When using policies, argument configurations and updates don't have to be done for every single queue or exchange. Policies simply ensure that all matching queues or exchanges come with the desirable preset arguments, suitable for their purpose, and also ensures that updates of one single argument are applied on all queues or exchanges bound to it.

### How can policies be used?

A policy can be set when you want to apply a TTL on a set of queues. Policies could be used when you want to delete single or multiple queues at once, or when you want to delete all messages from a queue.

## Policies in LavinMQ

A policy is applied when the pattern, a [regular expression](https://en.wikipedia.org/wiki/Regular_expression), matches a queue or exchange. As soon as a policy is created it will be applied to the matching queues and/or exchanges and its arguments will be amended to the definitions. As the match occurs continuously changes can easily be applied to multiple queues that are up and running. For example, if a TTL is to be set on a group of queues, or if multiple queues are to be deleted or purged at once. A policy is also applied every time an exchange or queue is created if a match exists. Only one policy can be matched to every queue or exchange at once, but one policy may be set to apply multiple arguments.

Policies are created per vhost, with a pattern that defines where it will be applied and a parameter that defines what the policy will do. The parameter is entered as a key (the parameter name) and a value (the parameter value), also called a key-value pair. Policies can be set from a terminal using [lavinmqctl](/documentation/lavinmqctl) or by using the [HTTP API](https://docs.lavinmq.com/http-api.html#tag/policies) or [Web Management Interface](/documentation/management-interface-overview).

## How to create and view policies

To create a policy we will define the following:

- Name of the policy
- The vhost it lives in, default is /
- A pattern or exact match to determine which queues or exchanges it applies to
- A definition consisting of one or several key-value pairs
- A priority, how it will be applied in relation to other policies, default is 0

Policies can be viewed and created from either of the following:

- The LavinMQ management interface
- A terminal using lavinmqctl
- The HTTP API

## Policies in the LavinMQ management interface

Policies are listed under “policies” in the LavinMQ menu. On the same page in the Add/update section, a new policy can be created.

Filling the name, patterns, and definition fields is mandatory. Note that below the definitions box a selection of keys are listed that can be added to the policy by clicking them. Values have to be added to the definitions box for every key added.

A priority should be used if multiple policies are used where the patterns overlap.

<img class="border border-[#414040]" src="/img/docs/docs-dm/policies-dm.png"/>

## Policies in lavinmqctl

The following commands are available for policies via [lavinmqctl](/documentation/lavinmqctl).

#### List Policies

List policies by using the command<br/> `lavinmqctl list_policies -p vhostname`

#### Create Policies

Create a policy by using the command <br/> `lavinmqctl set_policy <name> <pattern> <definition>`

| Example                                                                                                             |
| :------------------------------------------------------------------------------------------------------------------ |
| <code class="language-plaintext highlighter-rouge">lavinmqctl set_policy Policy2 '.\*' '{"message-ttl": 60}'</code> |

To specify vhost, priority, and exchange/queue-application use the following syntax<br/>
`lavinmqctl set_policy <name> <pattern> <definition> -p <vhost> --apply-o <queues|exchanges> --priority 8`

| Example                                                                                                                                                                                                                         |
| :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| <code class="language-plaintext highlighter-rouge">lavinmqctl set_policy Policy5 &#x27;queue_A&#x27; &#x27;{&#x22;message-ttl&#x22;: 60}&#x27; -p<br class="hidden lg:inline"/> zyxhvjpx --apply-to queues --priority 10</code> |

#### Delete Policies

Deleting policies is done with the command<br/> `lavinmqctl clear_policy <name>`

| Example                                                                                          |
| :----------------------------------------------------------------------------------------------- |
| <code class="language-plaintext highlighter-rouge">sudo lavinmqctl clear_policy 'Policy1'</code> |

## Policies with the HTTP API

The following commands are available for policies via [LavinMQ HTTP Api](https://docs.lavinmq.com/http-api.html#tag/policies).

#### List Policies

Policies can be listed with the following API call<br/>`curl -u USERNAME:PASSWORD -X GET https://SERVERNAME.lmq.cloudamqp.com/api/policies`

#### Create Policies

Policies can be added with the following API call<br/>`curl -XPUT -u USERNAME:PASSWORD --header "Content-Type: application/json" --data '{"pattern":"[PATTERN]","definition":{"key":value},"apply-to":"[queues|exchanges]"}' https://host/api/policies/[VHOST]/[POLICYNAME]`

#### Delete Policies

Policies can be deleted with the following API call <br/>`curl -u USERNAME:PASSWORD -X DELETE https://host/api/policies/[VHOST]/[POLICYNAME]`
