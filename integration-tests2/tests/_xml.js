import fs from "fs-extra"
import { XMLParser } from "fast-xml-parser"

function xml(ctx) {
  describe("xml", () => {
    beforeEach(async () => {
      // import a file
      let data = await fs.readFile("data/berlin_alexanderplatz_mini.xml", "utf-8")
      let res = await ctx.request.post("/store?await=true", data)
      expect(res.status).toBe(204)
    })
  
    afterEach(async () => {
      // clear data store
      let res = await ctx.request.delete("/store?await=true")
      expect(res.status).toBe(204)
    })

    it("returns contents of entire store", async () => {
      let res = await ctx.request.get("/store", { responseType: "text" })
      expect(res.status).toBe(200)

      const options = {
        ignoreAttributes: false
      }
      let parser = new XMLParser(options)
      let actual = parser.parse(res.data)
      let expected = parser.parse(await fs.readFile("data/berlin_alexanderplatz_mini.xml", "utf-8"))

      expect(actual).toEqual(expected)
    })
  })
}

export default xml
