# redisclient

Provides sync and async clients to communicate with redis servers using [nim-redisparser](https://github.com/xmonader/nim-redisparser)

## Executing commands


### Sync

```nim

  let con = open("localhost", 6379.Port)
  echo $con.execCommand("PING", @[])
  echo $con.execCommand("SET", @["auser", "avalue"])
  echo $con.execCommand("GET", @["auser"])
  echo $con.execCommand("SCAN", @["0"])

```

### Async

```nim
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
 
```


## Pipelining
You can use `enqueueCommand` and `commitCommands` to make use of redis pipelining
```nim
  con.enqueueCommand("PING", @[])
  con.enqueueCommand("PING", @[])
  con.enqueueCommand("PING", @[])
  
  echo $con.commitCommands()
```


## Roadmap
- [X] Pipelining
- [X] Async APIs
- [X] Friendlier API for builtin redis commands (adapted code from https://github.com/nim-lang/redis)