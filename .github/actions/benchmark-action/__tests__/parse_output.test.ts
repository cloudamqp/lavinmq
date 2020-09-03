import {parse_output} from "../src/parse_output"

it("should parse JSON string", async () => {
  const val = "{\"name\": \"test\", \"number\": 1}"
  const expected = {
    name: "test",
    number: 1
  }
  await expect(await parse_output(val)).toEqual(expected)
})

it("should parse JSON string containing other data", async () => {
  const val = "test\n {\"name\": \"test\", \"number\": 1} \n test \n"
  const expected = {
    name: "test",
    number: 1
  }
  await expect(await parse_output(val)).toEqual(expected)
})