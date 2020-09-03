import {benchmark} from '../src/benchmark'
import * as process from 'process'
import * as cp from 'child_process'
import * as path from 'path'

it("should echo hello", async () => {
  const expected = "hello"
  let res = await benchmark("echo hello")
  await expect(res.output.trim()).toEqual<string>(expected)
})

it("should echo value", async () => {
  const expected = "0"
  let res = await benchmark("echo 0")
  await expect(res.output.trim()).toEqual<string>(expected)
})

it("should echo JSON", async () => {
  const expected = {
    name: "test",
    number: 1
  }
  let res = await benchmark(path.join(__dirname, 'test.sh'))
  await expect(JSON.parse(res.output.trim())).toEqual<object>(expected)
})

test('test runs', async () => {
  process.env['INPUT_COMMAND'] = path.join(__dirname, '..', '..', 'avalanchemq', 'bin', 'avalanchemqperf throughput -j -q -z 1')
  const ip = path.join(__dirname, '..', 'lib', 'index.js')
  const options: cp.ExecSyncOptions = {
    env: process.env
  }
  console.log(cp.execSync(`node ${ip}`, options).toString())
})