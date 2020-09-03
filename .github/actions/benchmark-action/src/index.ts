import * as core from '@actions/core'
import {benchmark} from './benchmark'
import {parse_output} from './parse_output'
import {github_poster} from './github_poster'

const run = async () => {
  try {
    const res = await benchmark(core.getInput("command", {required: true}))
    const parsed_output = await parse_output(res.output.trim())
    await github_poster(parsed_output)
  } catch (error) {
    core.setFailed(error.message)
  }
}
run()
