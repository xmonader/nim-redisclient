# redisclient
# Copyright Ahmed T. Youssef
# nim redis client
import redisparser, tables, json, strutils, sequtils, net, asyncdispatch, asyncnet, os, strutils, deques, options, net
export redisparser

const
  CRLF* = "\r\n"

type
  RedisBase[TSocket] = ref object of RootObj
    socket: TSocket
    connected: bool
    timeout*: int

  Redis* = ref object of RedisBase[net.Socket]
    pipeline*: seq[RedisValue]

  AsyncRedis* = ref object of RedisBase[asyncnet.AsyncSocket]
    pipeline*: seq[RedisValue]

  RedisMessage* = object
    ## Pub/Sub
    channel*: string
    message*: string

proc `[]`*(obj: RedisValue, idx: int): RedisValue =
  if obj.kind != vkArray:
    raise newException(ValueError, "Invalid RedisValue kind, vkArray expected, got " & $obj.kind)
  result = obj.l[idx]

proc len*(obj: RedisValue): int =
  case obj.kind
  of vkStr:
    result = obj.s.len
  of vkError:
    result = obj.err.len
  of vkBulkStr:
    result = obj.bs.len
  of vkArray:
    result = obj.l.len
  else:
    raise newException(ValueError, "Invalid kind: " & $obj.kind)

when defined(ssl):
  proc SSLifyRedisConnectionNoVerify(redis: var Redis|AsyncRedis) =
    let ctx = newContext(verifyMode=CVerifyNone)
    ctx.wrapSocket(redis.socket)

proc open*(host = "localhost", port = 6379.Port, ssl=false, timeout=0): Redis =
  ## Open an asynchronous connection to a redis server.
  result = Redis(
    socket: newSocket(buffered = true),
  )
  result.pipeline = @[]
  result.timeout = timeout
  when defined(ssl):
    if ssl == true:
      SSLifyRedisConnectionNoVerify(result)
  result.socket.connect(host, port)
  result.connected = true

proc openAsync*(host = "localhost", port = 6379.Port, ssl=false, timeout=0): Future[AsyncRedis] {.async.} =
  ## Open an asynchronous connection to a redis server.
  result = AsyncRedis(
    socket: newAsyncSocket(buffered = true),
  )
  when defined(ssl):
    if ssl == true:
      SSLifyRedisConnectionNoVerify(result)
  result.pipeline = @[]
  result.timeout = timeout
  await result.socket.connect(host, port)
  result.connected = true

proc receiveManaged*(this:Redis|AsyncRedis, size=1): Future[string] {.multisync.} =
  result = newString(size)
  when this is Redis:
    if this.timeout == 0:
      discard this.socket.recv(result, size)
    else:
      discard this.socket.recv(result, size, this.timeout)
  else:
    discard await this.socket.recvInto(addr result[0], size)
  return result

proc readStream(this:Redis|AsyncRedis, breakAfter:string): Future[string] {.multisync.} =
  var data = ""
  while true:
    if data.endsWith(breakAfter):
      break
    let strRead = await this.receiveManaged()
    data &= strRead
  return data

proc readMany(this:Redis|AsyncRedis, count:int=1): Future[string] {.multisync.} =
  if count == 0:
    return ""
  let data = await this.receiveManaged(count)
  return data

proc readForm(this:Redis|AsyncRedis): Future[string] {.multisync.} =
  var form = ""
  while true:
    let b = await this.receiveManaged()
    form &= b
    case b[0]
    of '+', '-', ':':
      form &= await this.readStream(CRLF)
      return form
    of '$':
      let bulklenstr = await this.readStream(CRLF)
      let bulklenI = parseInt(bulklenstr.strip())
      form &= bulklenstr
      if bulklenI > 0:
        form &= await this.readMany(bulklenI)
        form &= await this.readStream(CRLF)
      return form
    of '*':
      let lenstr = await this.readStream(CRLF)
      form &= lenstr
      let lenstrAsI = parseInt(lenstr.strip())
      for i in countup(1, lenstrAsI):
        form &= await this.readForm()
      return form
    else:
      discard
  return form

proc execCommand*(this: Redis|AsyncRedis, command: string, args:seq[string]): Future[RedisValue] {.multisync.} =
  ## execute command `command` with arguments seq `args`

  let cmdArgs = concat(@[command], args)
  var cmdAsRedisValues = newSeq[RedisValue]()
  for cmd in cmdArgs:
    cmdAsRedisValues.add(RedisValue(kind:vkBulkStr, bs:cmd))
  var arr = RedisValue(kind:vkArray, l: cmdAsRedisValues)
  await this.socket.send(encode(arr))
  let form = await this.readForm()
  result = decodeString(form)

proc execCommand*(this: Redis|AsyncRedis, command: string): Future[RedisValue] {.multisync.} =
  ## exxecute command string
  return await execCommand(this, command, @[])

proc execCommand*(this: Redis|AsyncRedis, command: string, arg1:string): Future[RedisValue] {.multisync.} =
  return await execCommand(this, command, @[arg1])

proc execCommand*(this: Redis|AsyncRedis, command: string, arg1:string, args:seq[string]): Future[RedisValue] {.multisync.} =
  return await execCommand(this, command, concat(@[arg1], args))

proc enqueueCommand*(this:Redis|AsyncRedis, command:string, args: seq[string]): Future[void] {.multisync.} =
  ## enqueue command to redis pipeline
  let cmdArgs = concat(@[command], args)
  var cmdAsRedisValues = newSeq[RedisValue]()
  for cmd in cmdArgs:
    cmdAsRedisValues.add(RedisValue(kind:vkBulkStr, bs:cmd))
  var arr = RedisValue(kind:vkArray, l: cmdAsRedisValues)
  this.pipeline.add(arr)

proc commitCommands*(this:Redis|AsyncRedis) : Future[RedisValue] {.multisync.} =
  ## execute pipeline commands
  for cmd in this.pipeline:
    await this.socket.send(cmd.encode())
  var responses = newSeq[RedisValue]()
  for i in countup(0, len(this.pipeline)-1):
    responses.add(decodeString(await this.readForm()))
  this.pipeline = @[]
  return RedisValue(kind:vkArray, l:responses)


## HIGHER LEVEL INTERFACE
proc del*(this: Redis | AsyncRedis, keys: seq[string]): Future[RedisValue] {.multisync.} =
  ## Delete a key or multiple keys
  return await this.execCommand("DEL", keys)

proc exists*(this: Redis | AsyncRedis, key: string): Future[bool] {.multisync.} =
  ## Determine if a key exists
  let val = await this.execCommand("EXISTS", @[key])
  result = val.i == 1

proc expire*(this: Redis | AsyncRedis, key: string, seconds: int): Future[bool] {.multisync.} =
  ## Set a key's time to live in seconds. Returns `false` if the key could
  ## not be found or the timeout could not be set.
  let val = await this.execCommand("EXPIRE", key, @[$seconds])
  result = val.i == 1

proc expireAt*(this: Redis | AsyncRedis, key: string, timestamp: int): Future[bool] {.multisync.} =
  ## Set the expiration for a key as a UNIX timestamp. Returns `false`
  ## if the key could not be found or the timeout could not be set.
  let val = await this.execCommand("EXPIREAT", key, @[$timestamp])
  result = val.i == 1

proc keys*(this: Redis | AsyncRedis, pattern: string): Future[RedisValue] {.multisync.} =
  ## Find all keys matching the given pattern
  return await this.execCommand("KEYS", pattern)

proc scan*(this: Redis | AsyncRedis, position: BiggestInt): Future[RedisValue] {.multisync.} =
  ## Find all keys matching the given pattern and yield it to client in portions
  ## using default Redis values for MATCH and COUNT parameters
  return await this.execCommand("SCAN", $position)

proc scan*(this: Redis | AsyncRedis, position: BiggestInt, pattern: string): Future[RedisValue] {.multisync.} =
  ## Find all keys matching the given pattern and yield it to client in portions
  ## using cursor as a client query identifier. Using default Redis value for COUNT argument
  return await this.execCommand("SCAN", $position, @["MATCH", pattern])

proc scan*(this: Redis | AsyncRedis, position: BiggestInt, pattern: string, count: int): Future[RedisValue] {.multisync.} =
  ## Find all keys matching the given pattern and yield it to client in portions
  ## using cursor as a client query identifier.
  return await this.execCommand("SCAN", $position, @["MATCH", pattern, "COUNT", $count])

proc zdbScan*(this: Redis | AsyncRedis, position=""): Future[RedisValue] {.multisync.} =
  ## Find all keys matching the given pattern and yield it to client in portions
  ## using default Redis values for MATCH and COUNT parameters
  if position == "":
    return await this.execCommand("SCAN")
  else:
    return await this.execCommand("SCAN", position)

proc move*(this: Redis | AsyncRedis, key: string, db: int): Future[bool] {.multisync.} =
  ## Move a key to another database. Returns `true` on a successful move.
  let val =  await this.execCommand("MOVE", key, @[$db])
  result = val.i == 1

proc persist*(this: Redis | AsyncRedis, key: string): Future[bool] {.multisync.} =
  ## Remove the expiration from a key.
  ## Returns `true` when the timeout was removed.
  let val = await  this.execCommand("PERSIST", key)
  return val.i == 1

proc randomKey*(this: Redis | AsyncRedis): Future[RedisValue] {.multisync.} =
  ## Return a random key from the keyspace
  return await this.execCommand("RANDOMKEY")

proc rename*(this: Redis | AsyncRedis, key, newkey: string): Future[RedisValue] {.multisync.} =
  ## Rename a key.
  ##
  ## **WARNING:** Overwrites `newkey` if it exists!
  return await this.execCommand("RENAME", key, @[newkey])

proc renameNX*(this: Redis | AsyncRedis, key, newkey: string): Future[bool] {.multisync.} =
  ## Same as ``rename`` but doesn't continue if `newkey` exists.
  ## Returns `true` if key was renamed.
  let val = await this.execCommand("RENAMENX", key, @[newkey])
  result = val.i == 1

proc ttl*(this: Redis | AsyncRedis, key: string): Future[RedisValue] {.multisync.} =
  ## Get the time to live for a key
  return await this.execCommand("TTL", key)

proc keyType*(this: Redis | AsyncRedis , key: string): Future[RedisValue] {.multisync.} =
  ## Determine the type stored at key
  return await this.execCommand("TYPE", key)

# Strings
proc append*(this: Redis | AsyncRedis, key, value: string): Future[RedisValue] {.multisync.} =
  ## Append a value to a key
  return await this.execCommand("APPEND", key, @[value])

proc decr*(this: Redis | AsyncRedis, key: string): Future[RedisValue] {.multisync.} =
  ## Decrement the integer value of a key by one
  return await this.execCommand("DECR", key)

proc decrBy*(this: Redis | AsyncRedis, key: string, decrement: int): Future[RedisValue] {.multisync.} =
  ## Decrement the integer value of a key by the given number
  return await this.execCommand("DECRBY", key, @[$decrement])

proc mget*(this: Redis | AsyncRedis, keys: seq[string]): Future[RedisValue] {.multisync.} =
  ## Get the values of all given keys
  return await this.execCommand("MGET", keys)

proc get*(this: Redis | AsyncRedis, key: string): Future[RedisValue] {.multisync.} =
  ## Get the value of a key. Returns `redisNil` when `key` doesn't exist.
  return await this.execCommand("GET", key)

#TODO: BITOP
proc getBit*(this: Redis | AsyncRedis, key: string, offset: int): Future[RedisValue] {.multisync.} =
  ## Returns the bit value at offset in the string value stored at key
  return await this.execCommand("GETBIT", key, @[$offset])

proc bitCount*(this: Redis | AsyncRedis, key: string, limits: seq[string]): Future[RedisValue] {.multisync.} =
  ## Returns the number of set bits, optionally within limits
  return await this.execCommand("BITCOUNT", key, limits)

proc bitPos*(this: Redis | AsyncRedis, key: string, bit: int, limits: seq[string]): Future[RedisValue] {.multisync.} =
  ## Returns position of the first occurence of bit within limits
  var parameters: seq[string]
  newSeq(parameters, len(limits) + 1)
  parameters.add($bit)
  parameters.add(limits)
  return await this.execCommand("BITPOS", key, parameters)

proc getRange*(this: Redis | AsyncRedis, key: string, start, stop: int): Future[RedisValue] {.multisync.} =
  ## Get a substring of the string stored at a key
  return await this.execCommand("GETRANGE", key, @[$start, $stop])

proc getSet*(this: Redis | AsyncRedis, key: string, value: string): Future[RedisValue] {.multisync.} =
  ## Set the string value of a key and return its old value. Returns `redisNil`
  ## when key doesn't exist.
  return await this.execCommand("GETSET", key, @[value])

proc incr*(this: Redis | AsyncRedis, key: string): Future[RedisValue] {.multisync.} =
  ## Increment the integer value of a key by one.
  return await this.execCommand("INCR", key)

proc incrBy*(this: Redis | AsyncRedis, key: string, increment: int): Future[RedisValue] {.multisync.} =
  ## Increment the integer value of a key by the given number
  return await this.execCommand("INCRBY", key, @[$increment])

#TODO incrbyfloat

proc msetk*(
  this: Redis | AsyncRedis,
  keyValues: seq[tuple[key, value: string]]
): Future[RedisValue] {.multisync.} =
  ## Set mupltiple keys to multplie values
  var args: seq[string] = @[]
  for key, value in items(keyValues):
    args.add(key)
    args.add(value)
  return await this.execCommand("MSET", args)


proc setk*(this: Redis | AsyncRedis, key, value: string): Future[RedisValue] {.multisync.} =
  ## Set the string value of a key.
  ##
  ## NOTE: This function had to be renamed due to a clash with the `set` type.
  return await this.execCommand("SET", key, @[value])

proc setNX*(this: Redis | AsyncRedis, key, value: string): Future[bool] {.multisync.} =
  ## Set the value of a key, only if the key does not exist. Returns `true`
  ## if the key was set.
  let val = await this.execCommand("SETNX", key, @[value])
  result = val.i == 1

proc setBit*(this: Redis | AsyncRedis, key: string, offset: int,
             value: string): Future[RedisValue] {.multisync.} =
  ## Sets or clears the bit at offset in the string value stored at key
  return await this.execCommand("SETBIT", key, @[$offset, value])


proc setEx*(this: Redis | AsyncRedis, key: string, seconds: int, value: string): Future[RedisValue] {.multisync.} =
  ## Set the value and expiration of a key
  return await this.execCommand("SETEX", key, @[$seconds, value])

proc setRange*(this: Redis | AsyncRedis, key: string, offset: int,
               value: string): Future[RedisValue] {.multisync.} =
  ## Overwrite part of a string at key starting at the specified offset
  return await this.execCommand("SETRANGE", key, @[$offset, value])

proc strlen*(this: Redis | AsyncRedis, key: string): Future[RedisValue] {.multisync.} =
  ## Get the length of the value stored in a key. Returns 0 when key doesn't
  ## exist.
  return await this.execCommand("STRLEN", key)

# Hashes
proc hDel*(this: Redis | AsyncRedis, key, field: string): Future[bool] {.multisync.} =
  ## Delete a hash field at `key`. Returns `true` if the field was removed.
  let val =  await this.execCommand("HDEL", key, @[field])
  result = val.i == 1

proc hExists*(this: Redis | AsyncRedis, key, field: string): Future[bool] {.multisync.} =
  ## Determine if a hash field exists.
  let val = await this.execCommand("HEXISTS", key, @[field])
  result = val.i == 1

proc hGet*(this: Redis | AsyncRedis, key, field: string): Future[RedisValue] {.multisync.} =
  ## Get the value of a hash field
  return await this.execCommand("HGET", key, @[field])

proc hGetAll*(this: Redis | AsyncRedis, key: string): Future[RedisValue] {.multisync.} =
  ## Get all the fields and values in a hash
  return await this.execCommand("HGETALL", key)

proc hIncrBy*(this: Redis | AsyncRedis, key, field: string, incr: int): Future[RedisValue] {.multisync.} =
  ## Increment the integer value of a hash field by the given number
  return await this.execCommand("HINCRBY", key, @[field, $incr])

proc hKeys*(this: Redis | AsyncRedis, key: string): Future[RedisValue] {.multisync.} =
  ## Get all the fields in a hash
  return await this.execCommand("HKEYS", key)

proc hLen*(this: Redis | AsyncRedis, key: string): Future[RedisValue] {.multisync.} =
  ## Get the number of fields in a hash
  return await this.execCommand("HLEN", key)

proc hMGet*(this: Redis | AsyncRedis, key: string, fields: seq[string]): Future[RedisValue] {.multisync.} =
  ## Get the values of all the given hash fields
  return await this.execCommand("HMGET", key, fields)

proc hMSet*(this: Redis | AsyncRedis, key: string,
            fieldValues: seq[tuple[field, value: string]]): Future[RedisValue] {.multisync.} =
  ## Set multiple hash fields to multiple values
  var args = @[key]
  for field, value in items(fieldValues):
    args.add(field)
    args.add(value)
  return await this.execCommand("HMSET", args)

proc hSet*(this: Redis | AsyncRedis, key, field, value: string): Future[RedisValue] {.multisync.} =
  ## Set the string value of a hash field
  return await this.execCommand("HSET", key, @[field, value])

proc hSetNX*(this: Redis | AsyncRedis, key, field, value: string): Future[RedisValue] {.multisync.} =
  ## Set the value of a hash field, only if the field does **not** exist
  return await this.execCommand("HSETNX", key, @[field, value])

proc hVals*(this: Redis | AsyncRedis, key: string): Future[RedisValue] {.multisync.} =
  ## Get all the values in a hash
  return await this.execCommand("HVALS", key)

# Lists

proc bLPop*(this: Redis | AsyncRedis, keys: seq[string], timeout: int): Future[RedisValue] {.multisync.} =
  ## Remove and get the *first* element in a list, or block until
  ## one is available
  var args: seq[string]
  newSeq(args, len(keys) + 1)
  for i in items(keys):
    args.add(i)
  args.add($timeout)
  return await this.execCommand("BLPOP", args)

proc bRPop*(this: Redis | AsyncRedis, keys: seq[string], timeout: int): Future[RedisValue] {.multisync.} =
  ## Remove and get the *last* element in a list, or block until one
  ## is available.
  var args: seq[string]
  newSeq(args, len(keys) + 1)
  for i in items(keys):
    args.add(i)
  args.add($timeout)
  return await this.execCommand("BRPOP", args)

proc bRPopLPush*(this: Redis | AsyncRedis, source, destination: string,
                 timeout: int): Future[RedisValue] {.multisync.} =
  ## Pop a value from a list, push it to another list and return it; or
  ## block until one is available.
  ##
  ## http://redis.io/commands/brpoplpush
  return await this.execCommand("BRPOPLPUSH", source, @[destination, $timeout])

proc lIndex*(this: Redis | AsyncRedis, key: string, index: int): Future[RedisValue]  {.multisync.} =
  ## Get an element from a list by its index
  return await this.execCommand("LINDEX", key, @[$index])

proc lInsert*(this: Redis | AsyncRedis, key: string, before: bool, pivot, value: string):
              Future[RedisValue] {.multisync.} =
  ## Insert an element before or after another element in a list
  var pos = if before: "BEFORE" else: "AFTER"
  return await this.execCommand("LINSERT", key, @[pos, pivot, value])

proc lLen*(this: Redis | AsyncRedis, key: string): Future[RedisValue] {.multisync.} =
  ## Get the length of a list
  return await this.execCommand("LLEN", key)

proc lPop*(this: Redis | AsyncRedis, key: string): Future[RedisValue] {.multisync.} =
  ## Remove and get the first element in a list
  return await this.execCommand("LPOP", key)

proc lPush*(this: Redis | AsyncRedis, key, value: string, create: bool = true): Future[RedisValue] {.multisync.} =
  ## Prepend a value to a list. Returns the length of the list after the push.
  ## The ``create`` param specifies whether a list should be created if it
  ## doesn't exist at ``key``. More specifically if ``create`` is true, `LPUSH`
  ## will be used, otherwise `LPUSHX`.
  if create:
    return await this.execCommand("LPUSH", key, @[value])
  else:
    return await this.execCommand("LPUSHX", key, @[value])

proc lRange*(this: Redis | AsyncRedis, key: string, start, stop: int): Future[RedisValue] {.multisync.} =
  ## Get a range of elements from a list. Returns `nil` when `key`
  ## doesn't exist.
  return await this.execCommand("LRANGE", key, @[$start, $stop])

proc lRem*(this: Redis | AsyncRedis, key: string, value: string, count: int = 0): Future[RedisValue] {.multisync.} =
  ## Remove elements from a list. Returns the number of elements that have been
  ## removed.
  return await this.execCommand("LREM", key, @[$count, value])

proc lSet*(this: Redis | AsyncRedis, key: string, index: int, value: string): Future[RedisValue] {.multisync.} =
  ## Set the value of an element in a list by its index
  return await this.execCommand("LSET", key, @[$index, value])

proc lTrim*(this: Redis | AsyncRedis, key: string, start, stop: int): Future[RedisValue] {.multisync.}  =
  ## Trim a list to the specified range
  return await this.execCommand("LTRIM", key, @[$start, $stop])

proc rPop*(this: Redis | AsyncRedis, key: string): Future[RedisValue] {.multisync.} =
  ## Remove and get the last element in a list
  return await this.execCommand("RPOP", key)

proc rPopLPush*(this: Redis | AsyncRedis, source, destination: string): Future[RedisValue] {.multisync.} =
  ## Remove the last element in a list, append it to another list and return it
  return await this.execCommand("RPOPLPUSH", source, @[destination])

proc rPush*(this: Redis | AsyncRedis, key, value: string, create: bool = true): Future[RedisValue] {.multisync.} =
  ## Append a value to a list. Returns the length of the list after the push.
  ## The ``create`` param specifies whether a list should be created if it
  ## doesn't exist at ``key``. More specifically if ``create`` is true, `RPUSH`
  ## will be used, otherwise `RPUSHX`.
  if create:
    return await this.execCommand("RPUSH", key, @[value])
  else:
    return await this.execCommand("RPUSHX", key, @[value])

# Sets

proc sadd*(this: Redis | AsyncRedis, key: string, member: string): Future[RedisValue] {.multisync.} =
  ## Add a member to a set
  return await this.execCommand("SADD", key, @[member])

proc scard*(this: Redis | AsyncRedis, key: string): Future[RedisValue] {.multisync.} =
  ## Get the number of members in a set
  return await this.execCommand("SCARD", key)

proc sdiff*(this: Redis | AsyncRedis, keys: seq[string]): Future[RedisValue] {.multisync.} =
  ## Subtract multiple sets
  return await this.execCommand("SDIFF", keys)

proc sdiffstore*(this: Redis | AsyncRedis, destination: string,
                keys: seq[string]): Future[RedisValue] {.multisync.} =
  ## Subtract multiple sets and store the resulting set in a key
  return await this.execCommand("SDIFFSTORE", destination, keys)

proc sinter*(this: Redis | AsyncRedis, keys: seq[string]): Future[RedisValue] {.multisync.} =
  ## Intersect multiple sets
  return await this.execCommand("SINTER", keys)

proc sinterstore*(this: Redis | AsyncRedis, destination: string,
                 keys: seq[string]): Future[RedisValue] {.multisync.} =
  ## Intersect multiple sets and store the resulting set in a key
  return await this.execCommand("SINTERSTORE", destination, keys)

proc sismember*(this: Redis | AsyncRedis, key: string, member: string): Future[RedisValue] {.multisync.} =
  ## Determine if a given value is a member of a set
  return await this.execCommand("SISMEMBER", key, @[member])

proc smembers*(this: Redis | AsyncRedis, key: string): Future[RedisValue] {.multisync.} =
  ## Get all the members in a set
  return await this.execCommand("SMEMBERS", key)

proc smove*(this: Redis | AsyncRedis, source: string, destination: string,
           member: string): Future[RedisValue] {.multisync.} =
  ## Move a member from one set to another
  return await this.execCommand("SMOVE", source, @[destination, member])

proc spop*(this: Redis | AsyncRedis, key: string): Future[RedisValue] {.multisync.} =
  ## Remove and return a random member from a set
  return await this.execCommand("SPOP", key)

proc srandmember*(this: Redis | AsyncRedis, key: string): Future[RedisValue] {.multisync.} =
  ## Get a random member from a set
  return await this.execCommand("SRANDMEMBER", key)

proc srem*(this: Redis | AsyncRedis, key: string, member: string): Future[RedisValue] {.multisync.} =
  ## Remove a member from a set
  return await this.execCommand("SREM", key, @[member])

proc sunion*(this: Redis | AsyncRedis, keys: seq[string]): Future[RedisValue] {.multisync.} =
  ## Add multiple sets
  return await this.execCommand("SUNION", keys)

proc sunionstore*(this: Redis | AsyncRedis, destination: string,
                 key: seq[string]): Future[RedisValue] {.multisync.} =
  ## Add multiple sets and store the resulting set in a key
  return await this.execCommand("SUNIONSTORE", destination, key)

# Sorted sets

proc zadd*(this: Redis | AsyncRedis, key: string, score: int, member: string): Future[RedisValue] {.multisync.} =
  ## Add a member to a sorted set, or update its score if it already exists
  return await this.execCommand("ZADD", key, @[$score, member])

proc zcard*(this: Redis | AsyncRedis, key: string): Future[RedisValue] {.multisync.} =
  ## Get the number of members in a sorted set
  return await this.execCommand("ZCARD", key)

proc zcount*(this: Redis | AsyncRedis, key: string, min: string, max: string): Future[RedisValue] {.multisync.} =
  ## Count the members in a sorted set with scores within the given values
  return await this.execCommand("ZCOUNT", key, @[min, max])

proc zincrby*(this: Redis | AsyncRedis, key: string, increment: string,
             member: string): Future[RedisValue] {.multisync.}  =
  ## Increment the score of a member in a sorted set
  return await this.execCommand("ZINCRBY", key, @[increment, member])

proc zinterstore*(this: Redis | AsyncRedis, destination: string, numkeys: string,
                 keys: seq[string], weights: seq[string] = @[],
                 aggregate: string = ""): Future[RedisValue] {.multisync.} =
  ## Intersect multiple sorted sets and store the resulting sorted set in
  ## a new key
  var args: seq[string]
  let argsLen = 2 + len(keys) + (if len(weights) > 0: len(weights) + 1 else: 0) + (if len(aggregate) > 0: 1 + len(aggregate) else: 0)
  newSeq(args, argsLen)
  args.add(destination)
  args.add(numkeys)
  for i in items(keys):
    args.add(i)
  if weights.len != 0:
    args.add("WEIGHTS")
    for i in items(weights):
      args.add(i)
  if aggregate.len != 0:
    args.add("AGGREGATE")
    args.add(aggregate)
  return await this.execCommand("ZINTERSTORE", args)

proc zrange*(this: Redis | AsyncRedis, key: string, start: string, stop: string,
            withScores: bool = false): Future[RedisValue] {.multisync.} =
  ## Return a range of members in a sorted set, by index
  if not withScores:
    return await this.execCommand("ZRANGE", key, @[start, stop])
  else:
    return await this.execCommand("ZRANGE", key, @[start, stop, "WITHSCORES"])

proc zrangebyscore*(this: Redis | AsyncRedis, key: string, min: string, max: string,
                   withScores: bool = false, limit: bool = false,
                   limitOffset: int = 0, limitCount: int = 0): Future[RedisValue] {.multisync.} =
  ## Return a range of members in a sorted set, by score
  var args: seq[string]
  newSeq(args, 3 + (if withScores: 1 else: 0) + (if limit: 3 else: 0))
  args.add(key)
  args.add(min)
  args.add(max)

  if withScores: args.add("WITHSCORES")
  if limit:
    args.add("LIMIT")
    args.add($limitOffset)
    args.add($limitCount)

  return await this.execCommand("ZRANGEBYSCORE", args)

proc zrangebylex*(this: Redis | AsyncRedis, key: string, start: string, stop: string,
                  limit: bool = false, limitOffset: int = 0,
                  limitCount: int = 0): Future[RedisValue] {.multisync.} =
  ## Return a range of members in a sorted set, ordered lexicographically
  var args: seq[string]
  newSeq(args, 3 + (if limit: 3 else: 0))
  args.add(key)
  args.add(start)
  args.add(stop)
  if limit:
    args.add("LIMIT")
    args.add($limitOffset)
    args.add($limitCount)
  return await this.execCommand("ZRANGEBYLEX", args)

proc zrank*(this: Redis | AsyncRedis, key: string, member: string): Future[RedisValue] {.multisync.} =
  ## Determine the index of a member in a sorted set
  return await this.execCommand("ZRANK", key, @[member])

proc zrem*(this: Redis | AsyncRedis, key: string, member: string): Future[RedisValue] {.multisync.} =
  ## Remove a member from a sorted set
  return await this.execCommand("ZREM", key, @[member])

proc zremrangebyrank*(this: Redis | AsyncRedis, key: string, start: string,
                     stop: string): Future[RedisValue] {.multisync.} =
  ## Remove all members in a sorted set within the given indexes
  return await this.execCommand("ZREMRANGEBYRANK", key, @[start, stop])

proc zremrangebyscore*(this: Redis | AsyncRedis, key: string, min: string,
                      max: string): Future[RedisValue] {.multisync.} =
  ## Remove all members in a sorted set within the given scores
  return await this.execCommand("ZREMRANGEBYSCORE", key, @[min, max])

proc zrevrange*(this: Redis | AsyncRedis, key: string, start: string, stop: string,
               withScores: bool = false): Future[RedisValue] {.multisync.} =
  ## Return a range of members in a sorted set, by index,
  ## with scores ordered from high to low
  if withScores:
    return await this.execCommand("ZREVRANGE", key, @[start, stop, "WITHSCORES"])
  else:
    return await this.execCommand("ZREVRANGE", key, @[start, stop])

proc zrevrangebyscore*(this: Redis | AsyncRedis, key: string, min: string, max: string,
                   withScores: bool = false, limit: bool = false,
                   limitOffset: int = 0, limitCount: int = 0): Future[RedisValue] {.multisync.} =
  ## Return a range of members in a sorted set, by score, with
  ## scores ordered from high to low
  var args: seq[string]
  newSeq(args, 3 + (if withScores: 1 else: 0) + (if limit: 3 else: 0))
  args.add(key)
  args.add(min)
  args.add(max)

  if withScores: args.add("WITHSCORES")
  if limit:
    args.add("LIMIT")
    args.add($limitOffset)
    args.add($limitCount)
  return await this.execCommand("ZREVRANGEBYSCORE", args)

proc zrevrank*(this: Redis | AsyncRedis, key: string, member: string): Future[RedisValue] {.multisync.} =
  ## Determine the index of a member in a sorted set, with
  ## scores ordered from high to low
  return await this.execCommand("ZREVRANK", key, @[member])


proc zscore*(this: Redis | AsyncRedis, key: string, member: string): Future[RedisValue] {.multisync.} =
  ## Get the score associated with the given member in a sorted set
  return await this.execCommand("ZSCORE", key, @[member])

proc zunionstore*(this: Redis | AsyncRedis, destination: string, numkeys: string,
                 keys: seq[string], weights: seq[string] = @[],
                 aggregate: string = ""): Future[RedisValue] {.multisync.} =
  ## Add multiple sorted sets and store the resulting sorted set in a new key
  var args: seq[string]
  newSeq(args, 2 + len(keys) + (if len(weights) > 0: 1 + len(weights) else: 0) + (if len(aggregate) > 0: 1 + len(aggregate) else: 0))
  args.add(destination)
  args.add(numkeys)

  for i in items(keys):
    args.add(i)

  if weights.len != 0:
    args.add("WEIGHTS")
    for i in items(weights): args.add(i)

  if aggregate.len != 0:
    args.add("AGGREGATE")
    args.add(aggregate)
  return await this.execCommand("ZUNIONSTORE", args)

# HyperLogLog

proc pfadd*(this: Redis | AsyncRedis, key: string, elements: seq[string]): Future[RedisValue] {.multisync.} =
  ## Add variable number of elements into special 'HyperLogLog' set type
  return await this.execCommand("PFADD", key, elements)

proc pfcount*(this: Redis | AsyncRedis, key: string): Future[RedisValue] {.multisync.} =
  ## Count approximate number of elements in 'HyperLogLog'
  return await this.execCommand("PFCOUNT", key)

proc pfmerge*(this: Redis | AsyncRedis, destination: string, sources: seq[string]): Future[RedisValue] {.multisync.} =
  ## Merge several source HyperLogLog's into one specified by destKey
  return await this.execCommand("PFMERGE", destination, sources)

# Pub/Sub

#proc psubscribe*(this: Redis, pattern: openarray[string]): ???? =
#  ## Listen for messages published to channels matching the given patterns
# this.socket.send("PSUBSCRIBE $#\c\L" % pattern)

#  return ???
proc publish*(this: Redis | AsyncRedis , channel: string, message: string): Future[RedisValue] {.multisync.} =
  ## Post a message to a channel
  return await this.execCommand("PUBLISH", channel, @[message])

#proc punsubscribe*(this: Redis, [pattern: openarray[string], : string): ???? =
# ## Stop listening for messages posted to channels matching the given patterns
# this.socket.send("PUNSUBSCRIBE $# $#\c\L" % [[pattern.join(), ])
#  return ???

proc subscribe*(this: AsyncRedis, channels: seq[string]): Future[void] {.async.} =
  ## Listen for messages published to the given channels
  discard await this.execCommand("SUBSCRIBE", channels)

proc unsubscribe*(this: AsyncRedis, channels: seq[string]): Future[void] {.async.} =
  ## Stop listening for messages posted to the given channels
  discard await this.execCommand("UNSUBSCRIBE", channels)

proc nextMessage*(this: AsyncRedis): Future[RedisMessage] {.async.} =
  let
    form = await this.readForm()
    msg = decodeString(form)
  assert $msg[0] == "message"
  result = RedisMessage(
    channel: $msg[1],
    message: $msg[2]
  )
# Transactions

proc discardMulti*(this: Redis | AsyncRedis): Future[RedisValue] {.multisync.} =
  ## Discard all commands issued after MULTI
  return await this.execCommand("DISCARD")

# proc exec*(this: Redis | AsyncRedis): Future[RedisValue] {.multisync.} =
#   ## Execute all commands issued after MULTI
#   return await this.execCommand("EXEC")
#   #this.pipeline.enabled = false
#   # Will reply with +OK for MULTI/EXEC and +QUEUED for every command
#   # between, then with the results
#   result = awaitthis.flushPipeline(true)

# proc multi*(this: Redis | AsyncRedis): Future[RedisValue] {.multisync.} =
#   ## Mark the start of a transaction block
#  this.startPipelining()
#   return await this.execCommand("MULTI")


proc unwatch*(this: Redis | AsyncRedis): Future[RedisValue] {.multisync.} =
  ## Forget about all watched keys
  return await this.execCommand("UNWATCH")

proc watch*(this: Redis | AsyncRedis, key: seq[string]): Future[RedisValue] {.multisync.} =
  ## Watch the given keys to determine execution of the MULTI/EXEC block
  return await this.execCommand("WATCH", key)

# Connection

proc auth*(this: Redis | AsyncRedis, password: string): Future[RedisValue] {.multisync.} =
  ## Authenticate to the server
  return await this.execCommand("AUTH", password)

proc echoServ*(this: Redis | AsyncRedis, message: string): Future[RedisValue] {.multisync.} =
  ## Echo the given string
  return await this.execCommand("ECHO", message)

proc ping*(this: Redis | AsyncRedis): Future[RedisValue] {.multisync.} =
  ## Ping the server
  return await this.execCommand("PING")

proc quit*(this: Redis | AsyncRedis): Future[RedisValue] {.multisync.} =
  ## Close the connection
  let res = await this.execCommand("QUIT")
  this.socket.close()
  return res

proc select*(this: Redis | AsyncRedis, index: int): Future[RedisValue] {.multisync.} =
  ## Change the selected database for the current connection
  return await this.execCommand("SELECT", $index)

# Server

proc bgrewriteaof*(this: Redis | AsyncRedis): Future[RedisValue] {.multisync.} =
  ## Asynchronously rewrite the append-only file
  return await this.execCommand("BGREWRITEAOF")

proc bgsave*(this: Redis | AsyncRedis): Future[RedisValue] {.multisync.} =
  ## Asynchronously save the dataset to disk
  return await this.execCommand("BGSAVE")

proc configGet*(this: Redis | AsyncRedis, parameter: string): Future[RedisValue] {.multisync.} =
  ## Get the value of a configuration parameter
  return await this.execCommand("CONFIG", "GET", @[parameter])

proc configSet*(this: Redis | AsyncRedis, parameter: string, value: string): Future[RedisValue] {.multisync.} =
  ## Set a configuration parameter to the given value
  return await this.execCommand("CONFIG", "SET", @[parameter, value])

proc configResetStat*(this: Redis | AsyncRedis): Future[RedisValue] {.multisync.} =
  ## Reset the stats returned by INFO
  return await this.execCommand("CONFIG", "RESETSTAT")

proc dbsize*(this: Redis | AsyncRedis): Future[RedisValue] {.multisync.} =
  ## Return the number of keys in the selected database
  return await this.execCommand("DBSIZE")

proc debugObject*(this: Redis | AsyncRedis, key: string): Future[RedisValue] {.multisync.} =
  ## Get debugging information about a key
  return await this.execCommand("DEBUG", "OBJECT", @[key])

proc debugSegfault*(this: Redis | AsyncRedis): Future[RedisValue] {.multisync.} =
  ## Make the server crash
  return await this.execCommand("DEBUG", "SEGFAULT")

proc flushall*(this: Redis | AsyncRedis): Future[RedisValue] {.multisync.} =
  ## Remove all keys from all databases
  return await this.execCommand("FLUSHALL")

proc flushdb*(this: Redis | AsyncRedis): Future[RedisValue] {.multisync.} =
  ## Remove all keys from the current database
  return await this.execCommand("FLUSHDB")

proc info*(this: Redis | AsyncRedis): Future[RedisValue] {.multisync.} =
  ## Get information and statistics about the server
  return await this.execCommand("INFO")

proc lastsave*(this: Redis | AsyncRedis): Future[RedisValue] {.multisync.} =
  ## Get the UNIX time stamp of the last successful save to disk
  return await this.execCommand("LASTSAVE")

discard """
proc monitor*(this: Redis) =
  ## Listen for all requests received by the server in real time
 this.socket.send("MONITOR\c\L")
  raiseNoOK(r.readStatus(),this.pipeline.enabled)
"""

proc save*(this: Redis | AsyncRedis): Future[RedisValue] {.multisync.} =
  ## Synchronously save the dataset to disk
  return await this.execCommand("SAVE")

proc shutdown*(this: Redis | AsyncRedis): Future[RedisValue] {.multisync.} =
  ## Synchronously save the dataset to disk and then shut down the server
  return await this.execCommand("SHUTDOWN")

proc slaveof*(this: Redis | AsyncRedis, host: string, port: string): Future[RedisValue] {.multisync.} =
  ## Make the server a slave of another instance, or promote it as master
  return await this.execCommand("SLAVEOF", host, @[port])

# Streams

proc xack*(r: Redis | AsyncRedis, key, group: string, ids: varargs[string, `$`]):Future[RedisValue] {.multisync.} =
  ## Marks a pending message as correctly processed,
  ## effectively removing it from the pending entries list of the consumer group
  var args = @[group]
  args.add(ids)
  return await r.execCommand("XACK", key, args)

proc xadd*(r: Redis | AsyncRedis, key: string, fields: seq[tuple[field, value: string]], id = "*", maxlen: SomeInteger = 0,
  minId = "", nomkstream = false, approximate = false, limit = 0):Future[RedisValue] {.multisync.} =
  ## Appends new entries to a stream
  var args: seq[string]
  if nomkstream:
    args.add("NOMKSTREAM")
  if maxlen > 0:
    args.add("MAXLEN")
    if approximate:
      args.add("~")
    args.add($maxlen)
  if minid.len > 0:
    args.add("MINID")
    if approximate:
      args.add("~")
    args.add(minid)
  if limit > 0:
    args.add("LIMIT")
    args.add($limit)
  args.add(id)
  for (k, v) in fields:
    args.add(k)
    args.add(v)
  return await r.execCommand("XADD", key, args)

proc xadd*(r: Redis | AsyncRedis, key: string, field, value: string, id = "*", maxlen: SomeInteger = 0,
  minId = "", nomkstream = false, approximate = false, limit = 0):Future[RedisValue] {.multisync.} =
  ## Appends a new entry to a stream
  result = await r.xadd(key, @[(field, value)], id, maxlen, minId, nomkstream, approximate, limit)

proc xautoclaim*(r: Redis | AsyncRedis, key, group, consumer: string, minIdleTime: SomeInteger, start = "0", count: SomeInteger = 0, justId = false):Future[RedisValue] {.multisync.} =
  ## Changes (or acquires) ownership of messages in a consumer group, as if the messages were delivered to the specified consumer.
  var args = @[group, consumer, $minIdleTime, start]
  if count > 0:
    args.add("COUNT")
    args.add($count)
  if justId:
    args.add("JUSTID")
  return await r.execCommand("XAUTOCLAIM", key, args)

proc xclaim*(r: Redis | AsyncRedis, key, group, consumer: string, minIdleTime: SomeInteger, ids: seq[string], idle = 0, time = 0, retryCount: SomeInteger = 0, force = false, justId = false):Future[RedisValue] {.multisync.} =
  ## Changes (or acquires) ownership of a message in a consumer group, as if the message was delivered to the specified consumer.
  var args = @[group, consumer, $minIdleTime]
  args.add(ids)
  if idle > 0:
    args.add("IDLE")
    args.add($idle)
  if time > 0:
    args.add("TIME")
    args.add($time)
  if retryCount > 0:
    args.add("RETRYCOUNT")
    args.add($retryCount)
  if force:
    args.add("FORCE")
  if justId:
    args.add("JUSTID")
  return await r.execCommand("XCLAIM", key, args)

proc xdel*(r: Redis | AsyncRedis, key: string, ids: varargs[string, `$`]):Future[RedisValue] {.multisync.} =
  ## Removes the specified entries from the stream. Returns the number of items actually deleted
  var args: seq[string]
  args.add(ids)
  return await r.execCommand("XDEL", key, args)

proc xgroupCreate*(r: Redis | AsyncRedis, key, group: string, id = "$", mkstream = false):Future[RedisValue] {.multisync.} =
  ## Create a new consumer group.
  var args = @[key, group, id]
  if mkstream:
    args.add("MKSTREAM")
  return await r.execCommand("XGROUP", "CREATE", args)

proc xgroupSetId*(r: Redis | AsyncRedis, key, group: string, id = "$"):Future[RedisValue] {.multisync.} =
  ## Set the current group ID.
  var args = @[key, group, id]
  return await r.execCommand("XGROUP", "SETID", args)

proc xgroupDestroy*(r: Redis | AsyncRedis, key, group: string):Future[RedisValue] {.multisync.} =
  ## Set the current group ID.
  var args = @[key, group]
  return await r.execCommand("XGROUP", "DESTROY", args)

proc xgroupCreateConsumer*(r: Redis | AsyncRedis, key, group, consumer: string):Future[RedisValue] {.multisync.} =
  ## Set the current group ID.
  var args = @[key, group, consumer]
  return await r.execCommand("XGROUP", "CREATECONSUMER", args)

proc xgroupDelConsumer*(r: Redis | AsyncRedis, key, group, consumer: string):Future[RedisValue] {.multisync.} =
  ## Set the current group ID.
  var args = @[key, group, consumer]
  return await r.execCommand("XGROUP", "DELCONSUMER", args)

proc xinfoConsumers*(r: Redis | AsyncRedis, key, group: string):Future[RedisValue] {.multisync.} =
  ## Show consumers of ``groupname``
  var args = @[key, group]
  return await r.execCommand("XINFO", "CONSUMERS", args)

proc xinfoGroups*(r: Redis | AsyncRedis, key: string):Future[RedisValue] {.multisync.} =
  ## Show consumers of ``groupname``
  var args = @[key]
  return await r.execCommand("XINFO", "GROUPS", args)

proc xinfoStream*(r: Redis | AsyncRedis, key: string):Future[RedisValue] {.multisync.} =
  ## Show consumers of ``groupname``
  var args = @[key]
  return await r.execCommand("XINFO", "STREAM", args)

proc xinfoHelp*(r: Redis | AsyncRedis):Future[RedisValue] {.multisync.} =
  ## Show consumers of ``groupname``
  return await r.execCommand("XINFO", "HELP")

proc xlen*(r: Redis | AsyncRedis, key: string):Future[RedisValue] {.multisync.} =
  ## Return the number of entries in a stream
  return await r.execCommand("XLEN", key)

proc xpending*(r: Redis | AsyncRedis, key, group, start, stop = "", count: SomeInteger = 0, minIdleTime: SomeInteger = 0, consumer: string = ""):Future[RedisValue] {.multisync.} =
  ## Return information and entries from a stream consumer group pending entries list, that are messages fetched but never acknowledged.
  var args = @[group]
  if minIdleTime > 0:
    args.add("IDLE")
    args.add($minIdleTime)
  if start.len > 0:
    args.add(@[start, stop, $count])
    if consumer.len > 0:
      args.add(consumer)

  return await r.execCommand("XPENDING", key, args)

proc xrange*(r: Redis | AsyncRedis, key, start = "-", stop = "+", count: SomeInteger = 0):Future[RedisValue] {.multisync.} =
  ## Return a range of elements in a stream, with IDs matching the specified IDs interval
  var args = @[$start, $stop]
  if count > 0:
    args.add("COUNT")
    args.add($count)
  return await r.execCommand("XRANGE", key, args)

proc xread*(r: Redis | AsyncRedis, streams: seq[tuple[field, value: string]], count: SomeInteger = 0, blocking = 0):Future[RedisValue] {.multisync.} =
  ## Return never seen elements in multiple streams, with IDs greater than the ones reported by the caller for each stream. Can block.
  var args: seq[string]
  if count > 0:
    args.add("COUNT")
    args.add($count)
  if blocking > 0:
    args.add("BLOCK")
    args.add($blocking)
  args.add("STREAMS")
  var values: seq[string]
  for (k, v) in streams:
    args.add(k)
    values.add(v)
  args.add(values)
  return await r.execCommand("XREAD", args)

proc xreadGroup*(r: Redis | AsyncRedis, group, consumer: string, streams: seq[tuple[field, value: string]], count: SomeInteger = 0, blocking: SomeInteger = 0, noack = false):Future[RedisValue] {.multisync.} =
  ## Return new entries from a stream using a consumer group, or access the history of the pending entries for a given consumer. Can block.
  var args = @["GROUP", group, consumer]
  if count > 0:
    args.add("COUNT")
    args.add($count)
  if blocking > 0:
    args.add("BLOCK")
    args.add($blocking)
  if noack:
    args.add("NOACK")
  args.add("STREAMS")
  var values: seq[string]
  for (k, v) in streams:
    args.add(k)
    values.add(v)
  args.add(values)
  return await r.execCommand("XREADGROUP", args)

proc xrevrange*(r: Redis | AsyncRedis, key, stop = "+", start = "-", count: SomeInteger = 0):Future[RedisValue] {.multisync.} =
  ## Return a range of elements in a stream, with IDs matching the specified IDs interval,
  ## in reverse order (from greater to smaller IDs) compared to XRANGE
  var args = @[$stop, $start]
  if count > 0:
    args.add("COUNT")
    args.add($count)
  return await r.execCommand("XREVRANGE", key, args)

proc xtrim*(r: Redis | AsyncRedis, key: string, maxlen: SomeInteger = 0, minId = "", nomkstream = false, approximate = false, limit = 0):Future[RedisValue] {.multisync.} =
  ## Trims the stream to (approximately if '~' is passed) a certain size
  var args: seq[string]
  if maxlen > 0:
    args.add("MAXLEN")
    if approximate:
      args.add("~")
    args.add($maxlen)
  elif minid.len > 0:
    args.add("MINID")
    if approximate:
      args.add("~")
    args.add(minid)
  if limit > 0:
    args.add("LIMIT")
    args.add($limit)
  return await r.execCommand("XTRIM", key, args)