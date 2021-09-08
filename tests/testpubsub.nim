import redisclient, asyncdispatch

## run this test, and then open another connection

proc publish() {.async.} =
  var pub = await redisclient.openAsync("localhost")
  var i = 0
  while true:
    discard await pub.publish("mychannel", "hello world "  & $i)
    await sleepAsync(1000)
    inc(i)
    if i > 10:
      break

proc poll() {.async.} =
  var sub = await redisclient.openAsync("localhost")
  await sub.subscribe(@["mychannel"])

  while true:
    let msg = await sub.nextMessage()
    echo msg
    if msg.message == "hello world 10":
      break


asyncCheck publish()
waitFor poll()
