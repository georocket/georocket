import fs from "fs-extra"

function json(ctx) {
  describe("json", () => {
    beforeEach(async () => {
      // import a file
      let data = await fs.readFile("data/featurecollection.json", "utf-8")
      let res = await ctx.request.post("/store?await=true", data)
      expect(res.status).toBe(204)
    })
  
    afterEach(async () => {
      // clear data store
      let res = await ctx.request.delete("/store?await=true")
      expect(res.status).toBe(204)
    })

    it("returns contents of entire store", async () => {
      let res = await ctx.request.get("/store")
      expect(res.status).toBe(200)

      let expected = await fs.readFile("data/featurecollection.json", "utf-8")
      expect(res.data).toEqual(JSON.parse(expected))
    })

    it("returns Darmstadtium by string search", async () => {
      let res = await ctx.request.get("/store/?search=Darmstadtium")
      expect(res.status).toBe(200)
      expect(res.data.features).toHaveLength(1)
      expect(res.data.features[0].properties.name).toEqual("Darmstadtium")
    })

    it("returns Darmstadtium by EQ", async () => {
      let res = await ctx.request.get("/store/?search=EQ(name Darmstadtium)")
      expect(res.status).toBe(200)
      expect(res.data.features).toHaveLength(1)
      expect(res.data.features[0].properties.name).toEqual("Darmstadtium")
    })

    it("returns Fraunhofer IGD by EQ", async () => {
      let res = await ctx.request.get("/store/?search=EQ(name \"Fraunhofer IGD\")")
      expect(res.status).toBe(200)
      expect(res.data.features).toHaveLength(1)
      expect(res.data.features[0].properties.name).toEqual("Fraunhofer IGD")
    })

    it("returns 404 if nothing was found", async () => {
      let res = await ctx.request.get("/store/?search=foobar", { validateStatus: undefined })
      expect(res.status).toBe(404)
    })

    it("returns Fraunhofer IGD by bounding box", async () => {
      let res = await ctx.request.get("/store?search=8.6598,49.87423,8.6600,49.87426")
      expect(res.status).toBe(200)
      expect(res.data.features).toHaveLength(1)
      expect(res.data.features[0].properties.name).toEqual("Fraunhofer IGD")
    })

    it("returns 404 after store has been deleted", async () => {
      // clear data store
      let res = await ctx.request.delete("/store?await=true")
      expect(res.status).toBe(204)

      // should return 404 now
      let res1 = await ctx.request.get("/store", { validateStatus: undefined })
      expect(res1.status).toBe(404)
    })

    it("returns 400 if query contains syntax error", async () => {
      let res = await ctx.request.get("/store/?search=EQ(name Fraunhofer IGD)", { validateStatus: undefined })
      expect(res.status).toBe(400)
    })

    // TODO test tags (see XMLTests.groovy)

    // TODO test properties (see XMLTests.groovy)

    // TODO test tags+properties (see XMLTests.groovy)

    // TODO test get properties list (see XMLTests.groovy)

    // TODO test get list of generic attributes (see XMLTests.groovy)
  })
}

export default json
