
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
  echo "Opened async"
  var res = await con.execCommand("PING", @[])
  echo res
  res = await con.execCommand("SET", @["auser", "avalue"])
  echo res
  res = await con.execCommand("GET", @["auser"])
  echo res
  res = await con.execCommand("SCAN", @["0"])
  echo res
  res = await con.execCommand("SET", @["auser", "avalue"])
  echo res
  res = await con.execCommand("GET", @["auser"])
  echo res
  res = await con.execCommand("SCAN", @["0"])
  echo res 

  await con.enqueueCommand("PING", @[])
  await con.enqueueCommand("PING", @[])
  await con.enqueueCommand("PING", @[])
  res = await con.commitCommands()
  echo res


testSync()
waitFor testAsync()