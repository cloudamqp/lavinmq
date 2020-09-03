import {exec} from '@actions/exec'

export interface iBenchmark {
  code: number,
  output: string,
  error: string
}

export const benchmark = async (program: string) : Promise<iBenchmark> => {
  let output = "";
  let error = "";

  const options = {
    listeners: {
      stdout: (data: Buffer) => {
        output += data.toString();
      },
      stderr: (data: Buffer) => {
        error += data.toString();
      }
    }
  }
  let code = await(exec(program, [], options))
  return {
    code,
    output,
    error
  }
}
