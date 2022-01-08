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

    it("returns one building by string search", async () => {
      let res = await ctx.request.get("/store/?search=ID_147_D")
      expect(res.status).toBe(200)

      let parser = new XMLParser()
      let data = parser.parse(res.data)

      expect(Array.isArray(data.CityModel.cityObjectMember)).toBe(false)
      expect(data.CityModel.cityObjectMember["bldg:Building"]
        ["gen:stringAttribute"]["gen:value"]).toEqual("TestBuilding1")
    })

    it("returns other building by string search", async () => {
      let res = await ctx.request.get("/store/?search=TestBuilding2")
      expect(res.status).toBe(200)

      let parser = new XMLParser()
      let data = parser.parse(res.data)

      expect(Array.isArray(data.CityModel.cityObjectMember)).toBe(false)
      expect(data.CityModel.cityObjectMember["bldg:Building"]
        ["gen:stringAttribute"]["gen:value"]).toEqual("TestBuilding2")
    })

    it("returns other building by EQ", async () => {
      let res = await ctx.request.get("/store/?search=EQ(TestBuildingName TestBuilding2)")
      expect(res.status).toBe(200)

      let parser = new XMLParser()
      let data = parser.parse(res.data)

      expect(Array.isArray(data.CityModel.cityObjectMember)).toBe(false)
      expect(data.CityModel.cityObjectMember["bldg:Building"]
        ["gen:stringAttribute"]["gen:value"]).toEqual("TestBuilding2")
    })

    it("returns buildings by gml:id", async () => {
      let res = await ctx.request.get("/store/?search=EQ(gmlId ID_147_D)")
      expect(res.status).toBe(200)

      let parser = new XMLParser()
      let data = parser.parse(res.data)

      expect(Array.isArray(data.CityModel.cityObjectMember)).toBe(false)
      expect(data.CityModel.cityObjectMember["bldg:Building"]
        ["gen:stringAttribute"]["gen:value"]).toEqual("TestBuilding1")

      res = await ctx.request.get("/store/?search=EQ(gmlId ID_146_D)")
      expect(res.status).toBe(200)

      data = parser.parse(res.data)

      expect(Array.isArray(data.CityModel.cityObjectMember)).toBe(false)
      expect(data.CityModel.cityObjectMember["bldg:Building"]
        ["gen:stringAttribute"]["gen:value"]).toEqual("TestBuilding2")
    })

    it("returns one building by bounding box", async () => {
      let res = await ctx.request.get("/store/?search=13.04709829608855,52.33016111449572,13.047127397967273,52.330179231017645")
      expect(res.status).toBe(200)

      let parser = new XMLParser()
      let data = parser.parse(res.data)

      expect(Array.isArray(data.CityModel.cityObjectMember)).toBe(false)
      expect(data.CityModel.cityObjectMember["bldg:Building"]
        ["gen:stringAttribute"]["gen:value"]).toEqual("TestBuilding1")
    })
  })
}

export default xml
