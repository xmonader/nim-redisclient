
import strformat, tables, json, strutils, sequtils, hashes, net, asyncdispatch, asyncnet, os, strutils, parseutils, deques, options

import redisparser, redisclient

proc testSync() = 
  let con = open("localhost", 6379.Port)
  echo $con.execCommand("PING", @[])

  echo $con.execCommand("SET", @["auser", "avalue"])
  echo $con.execCommand("GET", @["auser"])
  echo $con.execCommand("SCAN", @["0"])

  con.enqueueCommand("PING", @[])
  con.enqueueCommand("PING", @[])
  con.enqueueCommand("PING", @[])

  echo $con.commitCommands()

  con.enqueueCommand("PING", @[])
  con.enqueueCommand("SET", @["auser", "avalue"])
  con.enqueueCommand("GET", @["auser"])
  con.enqueueCommand("SCAN", @["0"])
  echo $con.commitCommands()


proc testAsync() {.async.} =
  let con = await openAsync("localhost", 6379.Port)
  echo await con.execCommand("PING", @[])
  echo await con.execCommand("SET", @["auser", "avalue"])
  echo await con.execCommand("GET", @["auser"])
  echo await con.execCommand("SCAN", @["0"])
  echo await con.execCommand("SET", @["auser", "avalue"])
  echo await con.execCommand("GET", @["auser"])
  echo await con.execCommand("SCAN", @["0"])

  await con.enqueueCommand("PING", @[])
  await con.enqueueCommand("PING", @[])
  await con.enqueueCommand("PING", @[])
  echo await con.commitCommands()
 


proc testHighlevelAPI() =
  let con = open("localhost", 6379.Port)
  echo con.ping()
  discard con.setk("username", "ahmed")
  echo con.get("username")

# proc testZdbAPI() {.async.} =
#   let con = await openAsync("localhost", 9900.Port)
#   echo await con.ping()
#   echo await con.execCommand("SET", @["username", "ahmed"])
#   echo await con.get("username")

#   for i in countup(1, 100):
#     await con.execCommand("SET", @["key"& $i, $i])
#   echo await con.zdbScan("key2")
testSync()
waitFor testAsync()
testHighlevelAPI()
# waitFor testZdbAPI()


