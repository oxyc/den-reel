target "default" {
  context    = "."
  dockerfile = "Dockerfile"
  platforms  = ["linux/amd64"]
  # Re-pull every FROM: a base image served from the build cache is how a release inherits last month's libc.
  pull       = true
  cache-from = ["type=gha,scope=release-image"]
  # SLSA provenance and an SBOM, attached to the image index and so covered by its signature.
  attest = [
    "type=provenance,mode=max",
    "type=sbom",
  ]
}
