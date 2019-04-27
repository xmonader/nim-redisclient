# Package

version       = "0.1.1"
author        = "xmonader"
description   = "Redis client for Nim"
license       = "Apache2"
srcDir        = "src"

# Dependencies
requires "nim >= 0.18.0", "redisparser >= 0.1.1"



task genDocs, "Create code documentation for redisclient":
    exec "nim doc --threads:on --project src/redisclient.nim && rm -rf docs/api; mkdir -p docs && mv src/htmldocs docs/api "
