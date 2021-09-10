import redisclient, unittest

proc `&&`(s: string): auto = newRedisBulkString(s)

template syncTests() =
  let r = redisclient.open("localhost")
  let keys = r.keys("*")
  doassert keys.len == 0, "Don't want to mess up an existing DB."

  test "sorted sets":

    discard r.zadd("redisTest:myzset", 1, "one")
    discard r.zadd("redisTest:myzset", 2, "two")
    discard r.zadd("redisTest:myzset", 3, "three")

    check r.zcard("redisTest:myzset") == 3

    check r.zcount("redisTest:myzset", "-inf", "+inf") == 3
    check r.zcount("redisTest:myzset", "(1", "3") == 2

    discard r.zadd("redisTest:myzset", 2, "four")
    discard r.zincrby("redisTest:myzset", 2, "four")
    check r.zscore("redisTest:myzset", "four") == &&"4"
    check r.zrange("redisTest:myzset", 2, 3) == newRedisArray(@[
      &&"three",
      &&"3",
      &&"four",
      &&"4"
    ])
  # delete all keys in the DB at the end of the tests
  discard r.flushdb()
  discard r.quit()

suite "redis streams tests":
  syncTests()

when compileOption("threads"):
  proc threadFunc() {.thread.} =
    suite "redis streams threaded tests":
      syncTests()

  var th: Thread[void]
  createThread(th, threadFunc)
  joinThread(th)


