# redisclient
# Copyright Ahmed T. Youssef
# nim redis client 
import redisparser, strformat, tables, json, strutils, sequtils, hashes, net, asyncdispatch, asyncnet, os, strutils, parseutils, deques, options, net


type
  RedisBase[TSocket] = ref object of RootObj
    socket: TSocket
    connected: bool

  Redis* = ref object of RedisBase[net.Socket]
    pipeline*: seq[RedisValue]

  AsyncRedis* = ref object of RedisBase[asyncnet.AsyncSocket]
    pipeline*: seq[RedisValue]
    
when defined(ssl):
  proc SSLifyRedisConnectionNoVerify(redis: var Redis|AsyncRedis) = 
    let ctx = newContext(verifyMode=CVerifyNone)
    ctx.wrapSocket(redis.socket)

proc open*(host = "localhost", port = 6379.Port, ssl=false): Redis =
  result = Redis(
    socket: newSocket(buffered = true),
  )
  result.pipeline = @[]
  when defined(ssl):
    if ssl == true:
      SSLifyRedisConnectionNoVerify(result)
  result.socket.connect(host, port)

proc openAsync*(host = "localhost", port = 6379.Port, ssl=false): Future[AsyncRedis] {.async.} =
  ## Open an asynchronous connection to a redis server.
  result = AsyncRedis(
    socket: newAsyncSocket(buffered = true),
  )
  when defined(ssl):
    if ssl == true:
      SSLifyRedisConnectionNoVerify(result)
  result.pipeline = @[]
  await result.socket.connect(host, port)


proc receiveManaged*(this:Redis|AsyncRedis, size=1): Future[string] {.multisync.} =

  result = newString(size)
  when this is Redis:
    discard this.socket.recv(result, size)
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
    if b == "+":
      form &= await this.readStream(CRLF)
      return form
    elif b == "-":
      form &= await this.readStream(CRLF)
      return form
    elif b == ":":
      form &= await this.readStream(CRLF)
      return form
    elif b == "$":
      let bulklenstr = await this.readStream(CRLF)
      let bulklenI = parseInt(bulklenstr.strip()) 
      form &= bulklenstr
      if bulklenI == -1:
        form &= await this.readStream(CRLF)
      else:
        form &= await this.readMany(bulklenI)
        form &= await this.readStream(CRLF)
      return form
    elif b == "*":
        let lenstr = await this.readStream(CRLF)
        form &= lenstr
        let lenstrAsI = parseInt(lenstr.strip())
        for i in countup(1, lenstrAsI):
          form &= await this.readForm()
        return form
  return form


proc execCommand*(this: Redis|AsyncRedis, command: string, args:seq[string]): Future[RedisValue] {.multisync.} =
  let cmdArgs = concat(@[command], args)
  var cmdAsRedisValues = newSeq[RedisValue]()
  for cmd in cmdArgs:
    cmdAsRedisValues.add(RedisValue(kind:vkBulkStr, bs:cmd))
  var arr = RedisValue(kind:vkArray, l: cmdAsRedisValues)
  await this.socket.send(encode(arr))
  let form = await this.readForm()
  let val = decodeString(form)
  return val


proc enqueueCommand*(this:Redis|AsyncRedis, command:string, args: seq[string]): Future[void] {.multisync.} = 
  let cmdArgs = concat(@[command], args)
  var cmdAsRedisValues = newSeq[RedisValue]()
  for cmd in cmdArgs:
    cmdAsRedisValues.add(RedisValue(kind:vkBulkStr, bs:cmd))
  var arr = RedisValue(kind:vkArray, l: cmdAsRedisValues)
  this.pipeline.add(arr)

proc commitCommands*(this:Redis|AsyncRedis) : Future[RedisValue] {.multisync.} =
  for cmd in this.pipeline:
    await this.socket.send(cmd.encode())
  var responses = newSeq[RedisValue]()
  for i in countup(0, len(this.pipeline)-1):
    responses.add(decodeString(await this.readForm()))
  this.pipeline = @[]
  return RedisValue(kind:vkArray, l:responses)
