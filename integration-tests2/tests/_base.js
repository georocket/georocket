import { DockerComposeEnvironment } from "testcontainers"
import axios from "axios"
import json from "./_json.js"
import xml from "./_xml.js"

function setup(mode) {
  let environment
  let ctx = {}

  beforeAll(async () => {
    environment = await new DockerComposeEnvironment(".", `docker-compose-${mode}.yml`).up()
    
    let georocket = environment.getContainer("georocket_1")
    ctx.request = axios.create({
      baseURL: `http://${georocket.getHost()}:${georocket.getMappedPort(63020)}`
    })
  })

  afterAll(async () => {
    await environment?.down()
  })

  it("returns version info", async () => {
    let res = await ctx.request.get("/")
    expect(res.status).toBe(200)
    expect(res.headers["content-type"]).toMatch(/json/)
    expect(res.data.name).toEqual("GeoRocket")
    expect(res.data.version).toBeDefined()
  })

  it("returns 404 when store is empty", async () => {
    let res1 = await ctx.request.get("/store", { validateStatus: undefined })
    expect(res1.status).toBe(404)
    let res2 = await ctx.request.get("/store/?search=foobar", { validateStatus: undefined })
    expect(res2.status).toBe(404)
  })

  json(ctx)
  xml(ctx)
}

export default setup
