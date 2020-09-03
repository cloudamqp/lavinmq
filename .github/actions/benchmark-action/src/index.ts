import * as core from '@actions/core'
import * as github from '@actions/github'
import {benchmark} from './benchmark'
import {parse_output} from './parse_output'
import {github_poster} from './github_poster'
import * as Webhooks from '@octokit/webhooks'

export interface BenchmarkOutput {
  elapsed_seconds: number,
  avg_pub_rate: number,
  avg_consume_rate: number
}

const run = async () => {
  try {
    const res = await benchmark(core.getInput("command", {required: true}))
    const parsed_output = await parse_output(res.output.trim())
    let message_content = ""
    if (parsed_output === undefined) {
      message_content = "Error parsing output."
    }
    else {
      const pullPayload = github.context.payload as Webhooks.EventPayloads.WebhookPayloadPullRequest
      message_content = `
        Benchmark results for ${pullPayload.pull_request.head.ref}:

        Average consume rate: ${parsed_output.avg_consume_rate} msgs/s  
        Average publish rate: ${parsed_output.avg_pub_rate} msgs/s  
        Elapsed seconds: ${parsed_output.elapsed_seconds} msgs/s  
      `
    }
    await github_poster(message_content)
  } catch (error) {
    core.setFailed(error.message)
  }
}
run()
