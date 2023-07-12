You are interested in contributing to LavinMQ, but you are not sure how to? 
Right now, you can contribute to LavinMQ in one of two ways:

1. [Code contributions](#code-contributions)
2. [Non-code contributions](#non-code-contributions)

## Code contributions

The first step to making a code contribution, is starting a conversation around what you'd like to work on. You can start a conversation around an existing [issue](https://github.com/cloudamqp/lavinmq/issues) or a new one that you've created - refer to [reporting an issue](#report-an-issue). Next:

1. Fork, create feature branch
1. Submit pull request

### Develop
1. Run specs with `crystal spec`
1. Compile and run locally with `crystal run src/lavinmq.cr -- -D /tmp/amqp`
1. Pull js dependencies with `make js`
1. Build API docs with `make docs` (requires `npx`)
1. Build with `shards build`

### Release
1. Update `CHANGELOG.md`
1. Bump version in `shards.yml`
1. Create and push an annotated tag (`git tag -a v$(shards version)`), put the changelog of the version in the tagging message

## Non-Code contributions
Your schedule won't allow you make code contributions? Still fine, you can:

### [Report an issue](https://github.com/cloudamqp/lavinmq/issues/new)
- This could be an easily reproducible bug or even a feature request.
- If you spot an unexpected behaviour but you are not yet sure what the underlying bug is, the best place to post is [LavinMQ's community Slack](https://join.slack.com/t/lavinmq/shared_invite/zt-1v28sxova-wOyhOvDEKYVQMQpLePNUrg). This would allow us to interactively figure out what is going on. 

### [Give us some feedback](https://github.com/cloudamqp/lavinmq/issues/new)
We are also curious and happy to hear about your experience with LavinMQ. You can email us via contact@cloudamqp.com or reach us on Slack. Not sure what to write to us?

- You can write to us about your first impression of LavinMQ
- You can talk to us about features that are most important to you or your organization
- You can also just tell us what we can do better