import setup from "./_base.js"
import Minio from "minio"

let ctx = setup("s3")

beforeAll(async () => {
  // connect to minio and create bucket
  let minioContainer = ctx.environment.getContainer("s3_1")
  var minioClient = new Minio.Client({
    endPoint: minioContainer.getHost(),
    port: minioContainer.getMappedPort(9000),
    useSSL: false,
    accessKey: "minioadmin",
    secretKey: "minioadmin"
  })

  await minioClient.makeBucket("georocket")
})
