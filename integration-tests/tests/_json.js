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

    it("returns contents of entire store with scrolling", async () => {
      // page 1
      let res1 = await ctx.request.get("/store", {
        "params": {
          "scroll": true,
          "size": 1,
        }
      })
      expect(res1.status).toBe(200)
      expect(res1.headers).toHaveProperty("x-scroll-id")
      expect(res1.headers).toHaveProperty("x-hits", "1")

      // page 2
      let res2 = await ctx.request.get("/store", {
        "params": {
          "scroll": true,
          "size": 1,
          "scrollId": res1.headers["x-scroll-id"]
        }
      })
      expect(res2.status).toBe(200)
      expect(res2.headers).toHaveProperty("x-scroll-id")
      expect(res2.headers).toHaveProperty("x-hits", "1")

      // no more pages
      let res3 = await ctx.request.get("/store", {
        "params": {
          "scroll": true,
          "size": 1,
          "scrollId": res2.headers["x-scroll-id"]
        },
        "validateStatus": null
      })
      expect(res3.status).toBe(404)

      let expected = JSON.parse(await fs.readFile("data/featurecollection.json", "utf-8"))
      expect(res1.data).toHaveProperty("features", [expected.features[0]])
      expect(res2.data).toHaveProperty("features", [expected.features[1]])
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

    it("returns Darmstadtium by tag", async () => {
      // searching by tag should yield no result at the beginning
      let res = await ctx.request.get("/store/?search=mytag1", { validateStatus: undefined })
      expect(res.status).toBe(404)

      // add two tags
      res = await ctx.request.put("/store/?search=EQ(name Darmstadtium)&tags=mytag1,mytag2")
      expect(res.status).toBe(204)

      // feature with tag should now be returned
      res = await ctx.request.get("/store/?search=mytag1")
      expect(res.status).toBe(200)
      expect(res.data.features).toHaveLength(1)
      expect(res.data.features[0].properties.name).toEqual("Darmstadtium")

      res = await ctx.request.get("/store/?search=mytag2")
      expect(res.status).toBe(200)
      expect(res.data.features).toHaveLength(1)
      expect(res.data.features[0].properties.name).toEqual("Darmstadtium")

      // remove one of the tags
      res = await ctx.request.delete("/store/?search=mytag1&tags=mytag2")
      expect(res.status).toBe(204)

      // searching by the removed tag should now yield no result again
      res = await ctx.request.get("/store/?search=mytag2", { validateStatus: undefined })
      expect(res.status).toBe(404)

      // ... but the other tag should still work
      res = await ctx.request.get("/store/?search=mytag1")
      expect(res.status).toBe(200)
      expect(res.data.features).toHaveLength(1)
      expect(res.data.features[0].properties.name).toEqual("Darmstadtium")

      // remove the other tag too
      res = await ctx.request.delete("/store/?search=Darmstadtium&tags=mytag1")
      expect(res.status).toBe(204)

      // no feature should be returned now
      res = await ctx.request.get("/store/?search=mytag1", { validateStatus: undefined })
      expect(res.status).toBe(404)
    })

    it("can handle multiple tags on multiple features", async () => {
      // searching by tag should yield no result at the beginning
      let res = await ctx.request.get("/store/?search=mytag1", { validateStatus: undefined })
      expect(res.status).toBe(404)

      // add multiple tags to all features
      res = await ctx.request.put("/store/?tags=mytag1,mytag2,mytag3")
      expect(res.status).toBe(204)

      // all features should now be returned
      res = await ctx.request.get("/store/?search=mytag1")
      expect(res.status).toBe(200)
      expect(res.data.features).toHaveLength(2)

      res = await ctx.request.get("/store/?search=mytag2")
      expect(res.status).toBe(200)
      expect(res.data.features).toHaveLength(2)

      res = await ctx.request.get("/store/?search=mytag3")
      expect(res.status).toBe(200)
      expect(res.data.features).toHaveLength(2)

      // remove one of the tags from all features
      res = await ctx.request.delete("/store/?tags=mytag3")
      expect(res.status).toBe(204)

      // all features should now be returned (except for the removed tag)
      res = await ctx.request.get("/store/?search=mytag1")
      expect(res.status).toBe(200)
      expect(res.data.features).toHaveLength(2)

      res = await ctx.request.get("/store/?search=mytag2")
      expect(res.status).toBe(200)
      expect(res.data.features).toHaveLength(2)

      res = await ctx.request.get("/store/?search=mytag3", { validateStatus: undefined })
      expect(res.status).toBe(404)

      // remove a tag from one feature
      res = await ctx.request.delete("/store/?search=Darmstadtium&tags=mytag1")
      expect(res.status).toBe(204)

      // search should only return one result
      res = await ctx.request.get("/store/?search=mytag1")
      expect(res.status).toBe(200)
      expect(res.data.features).toHaveLength(1)

      // but still two results for the other tag
      res = await ctx.request.get("/store/?search=mytag2")
      expect(res.status).toBe(200)
      expect(res.data.features).toHaveLength(2)
    })

    it("can delete tag that does not exist", async () => {
      let res = await ctx.request.delete("/store/?tags=mytag1")
      expect(res.status).toBe(204)
    })

    it("can handle duplicate tags", async () => {
      // searching by tag should yield no result at the beginning
      let res = await ctx.request.get("/store/?search=mytag1", { validateStatus: undefined })
      expect(res.status).toBe(404)

      // add tag to all features
      res = await ctx.request.put("/store/?tags=mytag1")
      expect(res.status).toBe(204)

      // all features should now be returned
      res = await ctx.request.get("/store/?search=mytag1")
      expect(res.status).toBe(200)
      expect(res.data.features).toHaveLength(2)

      // add the same tag to all features again
      res = await ctx.request.put("/store/?tags=mytag1")
      expect(res.status).toBe(204)

      // all features should now be returned
      res = await ctx.request.get("/store/?search=mytag1")
      expect(res.status).toBe(200)
      expect(res.data.features).toHaveLength(2)

      // remove the tag
      res = await ctx.request.delete("/store/?tags=mytag1")
      expect(res.status).toBe(204)

      // no feature should be returned
      res = await ctx.request.get("/store/?search=mytag1", { validateStatus: undefined })
      expect(res.status).toBe(404)

      // ... but the features should still be in the store
      res = await ctx.request.get("/store")
      expect(res.status).toBe(200)
      expect(res.data.features).toHaveLength(2)
    })

    it("returns Darmstadtium by property", async () => {
      // searching by property should yield no result at the beginning
      let res = await ctx.request.get("/store/?search=EQ(myprop1 myvalue1)", { validateStatus: undefined })
      expect(res.status).toBe(404)

      // add two properties
      res = await ctx.request.put("/store/?search=EQ(name Darmstadtium)&properties=myprop1:myvalue1,myprop2:myvalue2")
      expect(res.status).toBe(204)

      // feature with property should now be returned
      res = await ctx.request.get("/store/?search=EQ(myprop1 myvalue1)")
      expect(res.status).toBe(200)
      expect(res.data.features).toHaveLength(1)
      expect(res.data.features[0].properties.name).toEqual("Darmstadtium")

      res = await ctx.request.get("/store/?search=myvalue1")
      expect(res.status).toBe(200)
      expect(res.data.features).toHaveLength(1)
      expect(res.data.features[0].properties.name).toEqual("Darmstadtium")

      res = await ctx.request.get("/store/?search=EQ(myprop2 myvalue2)")
      expect(res.status).toBe(200)
      expect(res.data.features).toHaveLength(1)
      expect(res.data.features[0].properties.name).toEqual("Darmstadtium")

      res = await ctx.request.get("/store/?search=myvalue2")
      expect(res.status).toBe(200)
      expect(res.data.features).toHaveLength(1)
      expect(res.data.features[0].properties.name).toEqual("Darmstadtium")

      // feature should not be returned if value is wrong
      res = await ctx.request.get("/store/?search=EQ(myprop2 myvalueZZZ)", 
        { validateStatus: undefined })
      expect(res.status).toBe(404)

      // remove one of the properties
      res = await ctx.request.delete("/store/?search=myvalue1&properties=myprop2")
      expect(res.status).toBe(204)

      // searching by the removed property should now yield no result again
      res = await ctx.request.get("/store/?search=myvalue2", { validateStatus: undefined })
      expect(res.status).toBe(404)

      // ... but the other property should still work
      res = await ctx.request.get("/store/?search=myvalue1")
      expect(res.status).toBe(200)
      expect(res.data.features).toHaveLength(1)
      expect(res.data.features[0].properties.name).toEqual("Darmstadtium")

      // remove the other property too
      res = await ctx.request.delete("/store/?search=Darmstadtium&properties=myprop1")
      expect(res.status).toBe(204)

      // no feature should be returned now
      res = await ctx.request.get("/store/?search=myprop1", { validateStatus: undefined })
      expect(res.status).toBe(404)
    })

    it("can update property", async() => {
      // searching by property should yield no result at the beginning
      let res = await ctx.request.get("/store/?search=EQ(myprop1 myvalue1)", { validateStatus: undefined })
      expect(res.status).toBe(404)

      // add a property
      res = await ctx.request.put("/store/?search=EQ(name Darmstadtium)&properties=myprop1:myvalue1")
      expect(res.status).toBe(204)

      // feature with property should now be returned
      res = await ctx.request.get("/store/?search=EQ(myprop1 myvalue1)")
      expect(res.status).toBe(200)
      expect(res.data.features).toHaveLength(1)
      expect(res.data.features[0].properties.name).toEqual("Darmstadtium")

      // feature should not be returned if value is wrong
      res = await ctx.request.get("/store/?search=EQ(myprop1 myvalue2)", { validateStatus: undefined })
      expect(res.status).toBe(404)

      // modify property
      res = await ctx.request.put("/store/?search=EQ(name Darmstadtium)&properties=myprop1:myvalue2")
      expect(res.status).toBe(204)

      // feature with property and new value should now be returned
      res = await ctx.request.get("/store/?search=EQ(myprop1 myvalue2)")
      expect(res.status).toBe(200)
      expect(res.data.features).toHaveLength(1)
      expect(res.data.features[0].properties.name).toEqual("Darmstadtium")

      // feature should not be returned if value is old
      res = await ctx.request.get("/store/?search=EQ(myprop1 myvalue1)", { validateStatus: undefined })
      expect(res.status).toBe(404)
    })

    it("can delete property that does not exist", async () => {
      let res = await ctx.request.delete("/store/?properties=myprop1")
      expect(res.status).toBe(204)
    })

    it("returns empty property list", async () => {
      let res = await ctx.request.get("/store/?property=myprop1", { validateStatus: undefined })
      expect(res.status).toBe(200)
      expect(res.data).toHaveLength(0)
    })

    it("returns property list", async () => {
      // add a property
      let res = await ctx.request.put("/store/?search=EQ(name Darmstadtium)&properties=myprop1:myvalue1")
      expect(res.status).toBe(204)

      res = await ctx.request.get("/store/?property=myprop1", { validateStatus: undefined })
      expect(res.status).toBe(200)
      expect(res.data).toEqual(["myvalue1"])

      // add the same property to another feature
      res = await ctx.request.put("/store/?search=EQ(name \"Fraunhofer IGD\")&properties=myprop1:myvalue2")
      expect(res.status).toBe(204)

      res = await ctx.request.get("/store/?property=myprop1", { validateStatus: undefined })
      expect(res.status).toBe(200)
      expect(res.data).toHaveLength(2)
      expect(res.data).toEqual(expect.arrayContaining(["myvalue1", "myvalue2"]))

      res = await ctx.request.get("/store/?search=Darmstadtium&property=myprop1", { validateStatus: undefined })
      expect(res.status).toBe(200)
      expect(res.data).toEqual(["myvalue1"])

      res = await ctx.request.get("/store/?search=\"Fraunhofer IGD\"&property=myprop1", { validateStatus: undefined })
      expect(res.status).toBe(200)
      expect(res.data).toEqual(["myvalue2"])
    })

    it("returns attribute list", async () => {
      let res = await ctx.request.get("/store/?search=EQ(name Darmstadtium)&attribute=name")
      expect(res.status).toBe(200)
      expect(res.data).toEqual(["Darmstadtium"])

      res = await ctx.request.get("/store/?search=EQ(name \"Fraunhofer IGD\")&attribute=name")
      expect(res.status).toBe(200)
      expect(res.data).toEqual(["Fraunhofer IGD"])

      res = await ctx.request.get("/store/?attribute=name")
      expect(res.status).toBe(200)
      expect(res.data).toHaveLength(2)
      expect(res.data).toEqual(expect.arrayContaining(["Darmstadtium", "Fraunhofer IGD"]))
    })
  })
}

export default json
