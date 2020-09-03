import {BenchmarkOutput} from './index'

export const parse_output = async (json : string) : Promise<BenchmarkOutput | undefined> => {
  const lines = json.split("\n")
  let return_value 
  lines.forEach(line => {
    try {
      return_value = JSON.parse(line)
      return
    } catch (error) {
    }
  })
  return return_value
}