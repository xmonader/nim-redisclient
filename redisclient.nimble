# Package

version       = "0.2.0"
author        = "xmonader"
description   = "Redis client for Nim"
license       = "Apache2"
srcDir        = "src"

# Dependencies
requires "nim >= 0.18.0", "redisparser >= 0.2.0"


task genDocs, "Create code documentation for redisclient":
    exec "nim doc --threads:on --project src/redisclient.nim && rm -rf docs/api; mkdir -p docs && mv src/htmldocs docs/api "
