import * as github from '@actions/github'
import * as core from '@actions/core'
import * as Webhooks from '@octokit/webhooks'

export const github_poster = async(body : string) => {
  const token = core.getInput('github_token');
  const octokit = github.getOctokit(token)

  if (github.context.eventName === "pull_request") {
    const pullPayload = github.context.payload as Webhooks.EventPayloads.WebhookPayloadPullRequest
    octokit.pulls.createReview({
      owner: github.context.repo.owner,
      repo: github.context.repo.repo,
      pull_number: pullPayload.number,
      body: body,
      event: "COMMENT"
    })
  }
} 