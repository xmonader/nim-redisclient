import redisclient, unittest, re

## Test cases borrowed from redis-py
## https://github.com/andymccurdy/redis-py/blob/master/tests/test_commands.py#L2691

const
  stream = "mystream"
  group = "mygroup"
  consumer = "myconsumer"

proc `&`(s: string): auto = newRedisString(s)
proc `&`(i: SomeInteger): auto = newRedisInt(i.int)
proc `&&`(s: string): auto = newRedisBulkString(s)

template syncTests() =
  let r = redisclient.open("localhost")
  let keys = r.keys("*")
  #doAssert keys.len == 0, "Don't want to mess up an existing DB."

  test "test xack":
    discard r.del(@[stream])
    var expected = 0
    # xack on a stream that doesn't exist
    check r.xack(stream, group, "0-0").getInt() == expected
    let
      id1 = r.xadd(stream, @[("one", "1")])
      id2 = r.xadd(stream, "two", "2")
      id3 = r.xadd(stream, "three", "3")
    # xack on a stream that doesn't exist
    check r.xack(stream, group, id1.getStr()).getInt() == expected
    check r.xgroupCreate(stream, group, "0").getStr() == "OK"

    discard r.xreadGroup(group, consumer, @[(stream, ">")])
    # xack returns the number of ack'd elements
    check r.xack(stream, group, id1.getStr()).getInt() == 1
    check r.xack(stream, group, id2.getStr(), id3.getStr()).getInt() == 2

  test "test xadd":
    let id1 = r.xadd(stream, "foo", "bar")
    assert re.match(id1.getStr(), re"[0-9]+\-[0-9]+")

    # explicit message id
    let id2 = "9999999999999999999-0"
    check id2 == r.xadd(stream, "foo", "bar", id=id2).getStr()

    # with maxlen, the list evicts the first message
    discard r.xadd(stream, "foo", "bar", maxlen=2)
    check r.xlen(stream).getInt() == 2

  test "test xadd nomlstream":
    # nomkstream option
    discard r.flushdb()
    discard r.xadd(stream, "some", "other", nomkstream=true)
    discard r.xadd(stream, "some", "other", nomkstream=false)
    assert r.xlen(stream).getInt() == 1
    discard r.xadd(stream, "foo", "bar")
    assert r.xlen(stream).getInt() == 2

  test "text xadd minlen and limit":
    discard r.xadd(stream, "foo", "bar")
    discard r.xadd(stream, "foo", "bar")
    discard r.xadd(stream, "foo", "bar")
    discard r.xadd(stream, "foo", "bar")
    check r.xadd(stream, "foo", "bar", maxlen=3, limit=2).isError()

    # limit can not be provided without maxlen or minid
    discard r.xadd(stream, "foo", "bar", limit=2).isError()

    # maxlen with a limit
    discard r.xadd(stream, "foo", "bar", maxlen=3, approximate=true, limit=2)

    discard r.del(@[stream])

    # maxlen and minid can not be provided together
    discard r.xadd(stream, "foo", "bar", maxlen=3, minid="2").isError()

    # minid with a limit
    discard r.xadd(stream, "foo", "bar")
    discard r.xadd(stream, "foo", "bar")
    discard r.xadd(stream, "foo", "bar")
    discard r.xadd(stream, "foo", "bar")
    discard r.xadd(stream, "foo", "bar", maxlen=3, approximate=true, limit=2)

    # pure minid
    let id2 = r.xadd(stream, "foo", "bar")
    discard r.xadd(stream, "foo", "bar", minid=id2.getStr())

    # minid approximate
    discard r.xadd(stream, "foo", "bar")
    discard r.xadd(stream, "foo", "bar", minid=id2.getStr(), approximate=true)

  test "test xautoclaim":
    const
      consumer1 = "consumer1"
      consumer2 = "consumer2"

    discard r.del(@[stream])

    let
      id1 = r.xadd(stream, "john", "wick")
      id2 = r.xadd(stream, "johny", "deff")
      message = r.xrange(stream, id1.getStr(), id1.getStr())

    discard r.xgroupCreate(stream, group, "0")

    # trying to claim a message that isn't already pending doesn't do anything
    var response = r.xautoclaim(stream, group, consumer2, minIdleTime=0)

    check response[0].getStr() == "0-0"

    # read the group as consumer1 to initially claim the messages
    discard r.xreadgroup(group, consumer1, @[(stream, ">")])

    # claim one message as consumer2
    response = r.xautoclaim(stream, group, consumer2, minIdleTime=0, count=1)
    check response[1] == message


    # reclaim the messages as consumer1, but use the justid argument
    # which only returns message ids
    response = r.xautoclaim(stream, group, consumer1, minIdleTime=0, justid=true)
    check response[1].getItems() == @[id1, id2]
    response = r.xautoclaim(stream, group, consumer1, minIdleTime=0, start = id2.getStr(), justid=true)
    check response[1].getItems() == @[id2]

  test "test xclaim":
    const
      consumer1 = "consumer1"
      consumer2 = "consumer2"
    discard r.del(@[stream])

    let
      id1 = r.xadd(stream, "john", "wick")
      messages = r.xrange(stream, id1.getStr(), id1.getStr())

    discard r.xgroupCreate(stream, group, "0")

    # trying to claim a message that isn't already pending doesn't do anything
    var response = r.xclaim(stream, group, consumer2, 0, ids = @[id1.getStr()])
    check response.len == 0

    # read the group as consumer1 to initially claim the messages
    discard r.xreadgroup(group, consumer1, @[(stream, ">")])

    # claim the message as consumer2
    response = r.xclaim(stream, group, consumer2, 0, ids = @[id1.getStr()])
    check response == messages
    # reclaim the message as consumer1, but use the justid argument
    # which only returns message ids
    response = r.xclaim(stream, group, consumer1, 0, ids = @[id1.getStr()], justid=true)
    check response.getItems() == @[id1]

  test "test xclaim trimmed":
    # xclaim should not raise an exception if the item is not there
    discard r.del(@[stream])
    discard r.xgroupCreate(stream, group, mkstream=true)

    # add a couple of new items
    let
      sid1 = r.xadd(stream, "item", "0")
      sid2 = r.xadd(stream, "item", "0")

    # read them from consumer1
    discard r.xreadgroup(group, "consumer1", @[(stream, ">")])

    # add a 3rd and trim the stream down to 2 items
    discard r.xadd(stream, "item", "0", maxlen=2)

    # xclaim them from consumer2
    # the item that is still in the stream should be returned
    let response = r.xclaim(stream, group, "consumer2", 0, ids = @[sid1.getStr(), sid2.getStr()])
    check response[0].isNil
    check response[1][0] == sid2
  test "test xdel":
    # deleting from an empty stream doesn't do anything
    check r.xdel(stream, 1).getInt() == 0
    let
      m1 = r.xadd(stream, "foo", "bar")
      m2 = r.xadd(stream, "foo", "bar")
      m3 = r.xadd(stream, "foo", "bar")

    # xdel returns the number of deleted elements
    check r.xdel(stream, m1.getStr()).getInt() == 1
    check r.xdel(stream, m2.getStr(), m3.getStr()).getInt() == 2

  test "test xgroup create":
    # tests xgroup_create and xinfo_groups
    discard r.del(@[stream])
    discard r.xadd(stream, "foo", "bar")
    # no group is setup yet, no info to obtain
    check r.xinfoGroups(stream).len == 0
    check r.xgroupCreate(stream, group, "0").getStr() == "OK"
    let expected = newRedisArray(@[
      &&"name",
      &&group,
      &&"consumers",
      &0,
      &&"pending",
      &0,
      &&"last-delivered-id",
      &&"0-0"
    ])
    check r.xinfoGroups(stream)[0] == expected

  test "test xgroup create mkstream":
    # tests xgroup_create and xinfo_groups
    discard r.del(@[stream])

    # an error is raised if a group is created on a stream that
    # doesn't already exist
    check r.xgroupCreate(stream, group, "0").isError()

    # however, with mkstream=True, the underlying stream is created automatically
    check r.xgroupCreate(stream, group, "0", mkstream = true).getStr() == "OK"

    let expected = newRedisArray(@[
      &&"name",
      &&group,
      &&"consumers",
      &0,
      &&"pending",
      &0,
      &&"last-delivered-id",
      &&"0-0"
    ])
    check r.xinfoGroups(stream)[0] == expected

  test "test xgroup createconsumer":
    discard r.del(@[stream])
    discard r.xadd(stream, "foo", "bar")
    discard r.xadd(stream, "foo", "bar")
    check r.xgroupCreate(stream, group, "0").getStr() == "OK"
    check r.xgroupCreateConsumer(stream, group, consumer).getInt() == 1

    # read all messages from the group
    discard r.xreadgroup(group, consumer, @[(stream, ">")])

    # deleting the consumer should return 2 pending messages
    check r.xgroupDelConsumer(stream, group, consumer).getInt() == 2

  test "test xgroup destroy":
    discard r.del(@[stream])
    discard r.xadd(stream, "foo", "bar")

    # destroying a nonexistent group returns False
    check r.xgroupDestroy(stream, group).getInt() == 0
    check r.xgroupCreate(stream, group, "0").getStr() == "OK"
    check r.xgroupDestroy(stream, group).getInt() == 1

  test "test xgroup setid":
    discard r.del(@[stream])
    let mid = r.xadd(stream, "foo", "bar")

    check r.xgroupCreate(stream, group, "0").getStr() == "OK"

    # advance the last_delivered_id to the message_id
    check r.xgroupSetId(stream, group, mid.getStr()).getStr() == "OK"

    let expected = newRedisArray(@[
      &&"name",
      &&group,
      &&"consumers",
      &0,
      &&"pending",
      &0,
      &&"last-delivered-id",
      mid
    ])
    check r.xinfoGroups(stream)[0] == expected

  test "text xinfo consumers":
    discard r.del(@[stream])
    discard r.xadd(stream, "foo", "bar")
    discard r.xadd(stream, "foo", "bar")
    discard r.xadd(stream, "foo", "bar")

    check r.xgroupCreate(stream, group, "0").getStr() == "OK"
    discard r.xreadgroup(group, "consumer1", @[(stream, ">")], count=1)
    discard r.xreadgroup(group, "consumer2", @[(stream, ">")])
    let info = r.xinfoConsumers(stream, group)

    check info.len == 2

    #[
    # FIXME temporary disable cuz `idle` is inconsistent

    let expected = newRedisArray(@[
      newRedisArray(@[
        &&"name", &&"consumer1",
        &&"pending", &1,
        &&"idle", &0,
      ]),
      newRedisArray(@[
        &&"name", &&"consumer2",
        &&"pending", &2,
        &&"idle", &0
      ])
    ])
    check info == expected
    ]#

  test "test xinfo stream":
    discard r.del(@[stream])
    let m1 = r.xadd(stream, "foo", "bar")
    discard r.xadd(stream, "foo", "bar")
    let m3 = r.xadd(stream, "foo", "bar")
    let
      expected = newRedisArray(@[
        &&"length", &3,
        &&"radix-tree-keys", &1,
        &&"radix-tree-nodes", &2,
        &&"last-generated-id", m3,
        &&"groups", &0,
        &&"first-entry", newRedisArray(@[
          &&(m1.getStr()),
          newRedisArray(@[
            &&"foo", &&"bar"
          ])
        ]),
        &&"last-entry", newRedisArray(@[
          &&(m3.getStr()),
          newRedisArray(@[
            &&"foo", &&"bar"
          ])
        ]),
      ])
    check r.xinfoStream(stream) == expected

  test "test xlen":
    discard r.del(@[stream])
    discard r.xadd(stream, "foo", "bar")
    discard r.xadd(stream, "foo", "bar")
    discard r.xadd(stream, "foo", "bar")
    check r.xlen(stream).getInt() == 3

  test "test xpending":
    discard r.del(@[stream])
    let
      m1 = r.xadd(stream, "foo", "bar")
      m2 = r.xadd(stream, "foo", "bar")
    discard r.xadd(stream, "foo", "bar")
    check r.xgroupCreate(stream, group, "0").getStr() == "OK"
    check r.xpending(stream, group)[0].getInt() == 0

    # read 1 message from the group with each consumer
    discard r.xreadgroup(group, "consumer1", @[(stream, ">")], count=1)
    discard r.xreadgroup(group, "consumer2", @[(stream, ">")], count=1)

    let expected = newRedisArray(@[
      &2,
      &&(m1.getStr()),
      &&(m2.getStr()),
      newRedisArray(@[
        newRedisArray(@[
          &&"consumer1", &&"1"
        ]),
        newRedisArray(@[
          &&"consumer2", &&"1"
        ])
      ])
    ])
    check r.xpending(stream, group) == expected

  test "test xpending range":
    discard r.del(@[stream])
    let
      m1 = r.xadd(stream, "foo", "bar")
      m2 = r.xadd(stream, "foo", "bar")
    check r.xgroupCreate(stream, group, "0").getStr() == "OK"
    check r.xpending(stream, group)[0].getInt() == 0

    # xpending range on a group that has no consumers yet
    check r.xpending(stream, group, "-", "+", 5).len == 0

    # read 1 message from the group with each consumer
    discard r.xreadgroup(group, "consumer1", @[(stream, ">")], count=1)
    discard r.xreadgroup(group, "consumer2", @[(stream, ">")], count=1)


    let response = r.xpending(stream, group, "-", "+", 5)
    check response.len == 2
    check response[0][0] == m1
    check response[0][1].getStr() == "consumer1"
    check response[1][0] == m2
    check response[1][1].getStr() == "consumer2"

  test "test xpending range idle":
    discard r.del(@[stream])
    discard r.xadd(stream, "foo", "bar")
    discard r.xadd(stream, "foo", "bar")
    check r.xgroupCreate(stream, group, "0").getStr() == "OK"

    # read 1 message from the group with each consumer
    discard r.xreadgroup(group, "consumer1", @[(stream, ">")], count=1)
    discard r.xreadgroup(group, "consumer2", @[(stream, ">")], count=1)

    var response = r.xpending(stream, group, "-", "+", 5)
    check response.len == 2

    response = r.xpending(stream, group, "-", "+", 5, minIdleTime=1000)
    check response.len == 0

  test "test xrange":
    discard r.del(@[stream])
    let
      m1 = r.xadd(stream, "foo", "bar")
      m2 = r.xadd(stream, "foo", "bar")
      m3 = r.xadd(stream, "foo", "bar")
      m4 = r.xadd(stream, "foo", "bar")

    var response = r.xrange(stream, start = m1.getStr())
    check response[0][0] == m1
    check response[1][0] == m2
    check response[2][0] == m3
    check response[3][0] == m4

    response = r.xrange(stream, start = m2.getStr(), stop = m3.getStr())
    check response[0][0] == m2
    check response[1][0] == m3

    response = r.xrange(stream, stop = m3.getStr())
    check response[0][0] == m1
    check response[1][0] == m2
    check response[2][0] == m3

    response = r.xrange(stream, stop = m2.getStr(), count=1)
    check response[0][0] == m1

  test "test xread":
    discard r.del(@[stream])
    let
      m1 = r.xadd(stream, "foo", "bar")
      m2 = r.xadd(stream, "bing", "baz")

    var expected = newRedisArray(@[
      &&stream,
      newRedisArray(@[
        r.xrange(stream, m1.getStr(), m1.getStr())[0],
        r.xrange(stream, m2.getStr(), m2.getStr())[0]
      ])
    ])
    # xread starting at 0 returns both messages
    check r.xread(@[(stream, "0")])[0] == expected

    expected = newRedisArray(@[
      &&stream,
      newRedisArray(@[
        r.xrange(stream, m1.getStr(), m1.getStr())[0]
      ])
    ])
    # xread starting at 0 and count=1 returns only the first message
    check r.xread(@[(stream, "0")], count=1)[0] == expected

    expected = newRedisArray(@[
      &&stream,
      newRedisArray(@[
        r.xrange(stream, m2.getStr(), m2.getStr())[0]
      ])
    ])
    # xread starting at m1 returns only the second message
    check r.xread(@[(stream, m1.getStr())])[0] == expected

    # xread starting at the last message returns an empty list
    check r.xread(@[(stream, m2.getStr())]).isNil
  test "test xreadgroup":
    discard r.del(@[stream])
    let
      m1 = r.xadd(stream, "foo", "bar")
      m2 = r.xadd(stream, "bing", "baz")
    check r.xgroupCreate(stream, group, "0").getStr() == "OK"

    var expected = newRedisArray(@[
      &&stream,
      newRedisArray(@[
        r.xrange(stream, m1.getStr(), m1.getStr())[0],
        r.xrange(stream, m2.getStr(), m2.getStr())[0]
      ])
    ])
    # xreadgroup starting at 0 returns both messages
    check r.xreadGroup(group, "consumer1", @[(stream, ">")])[0] == expected

    check r.xgroupDestroy(stream, group).getInt() == 1
    check r.xgroupCreate(stream, group, "0").getStr() == "OK"

    expected = newRedisArray(@[
      &&stream,
      newRedisArray(@[
        r.xrange(stream, m1.getStr(), m1.getStr())[0]
      ])
    ])
    # xreadgroup with count=1 returns only the first message
    check r.xreadGroup(group, "consumer1", @[(stream, ">")], count=1)[0] == expected

    check r.xgroupDestroy(stream, group).getInt() == 1

    # create the group using $ as the last id meaning subsequent reads
    # will only find messages added after this
    check r.xgroupCreate(stream, group, "$").getStr() == "OK"

    check r.xreadGroup(group, "consumer1", @[(stream, ">")]) == nil

    # xreadgroup with noack does not have any items in the PEL
    check r.xgroupDestroy(stream, group) == &1
    check r.xgroupCreate(stream, group, "0").getStr() == "OK"

    check r.xreadGroup(group, "consumer1", @[(stream, ">")], noack=true)[0][1].len == 2

    # now there should be nothing pending
    check r.xreadGroup(group, "consumer1", @[(stream, ">")], noack=true) == nil

    #check r.xgroupDestroy(stream, group) == &1
    #check r.xgroupCreate(stream, group, "0") == OK
    # delete all the messages in the stream
  test "test xrevrange":
    discard r.del(@[stream])
    let
      m1 = r.xadd(stream, "foo", "bar")
      m2 = r.xadd(stream, "foo", "bar")
      m3 = r.xadd(stream, "foo", "bar")
      m4 = r.xadd(stream, "foo", "bar")

    var response = r.xrevrange(stream, stop = m4.getStr())

    check response[0][0] == m4
    check response[1][0] == m3
    check response[2][0] == m2
    check response[3][0] == m1

    response = r.xrevrange(stream, start = m2.getStr(), stop = m3.getStr())
    check response[0][0] == m3
    check response[1][0] == m2

    response = r.xrevrange(stream, start = m3.getStr())
    check response[0][0] == m4
    check response[1][0] == m3

    response = r.xrevrange(stream, start = m2.getStr(), count=1)
    check response[0][0] == m4

  test "test xtrim":
    discard r.del(@[stream])

    # trimming an empty key doesn't do anything
    check r.xtrim(stream, 1000).getInt() == 0

    discard r.xadd(stream, "foo", "bar")
    discard r.xadd(stream, "foo", "bar")
    discard r.xadd(stream, "foo", "bar")
    discard r.xadd(stream, "foo", "bar")

    # trimming an amount large than the number of messages
    # doesn't do anything
    check r.xtrim(stream, 5).getInt() == 0

    # 1 message is trimmed
    check r.xtrim(stream, 3).getInt() == 1

  test "test xtrim minlen amdn lenght args":
    discard r.del(@[stream])
    discard r.xadd(stream, "foo", "bar")
    discard r.xadd(stream, "foo", "bar")
    discard r.xadd(stream, "foo", "bar")
    discard r.xadd(stream, "foo", "bar")

    check r.xtrim(stream, 3, limit=2).isError()

    # maxlen with a limit
    check r.xtrim(stream, 3, approximate=true, limit=2).getInt() == 0

    discard r.del(@[stream])
    check r.xtrim(stream, maxlen=3, minid="sometestvalue").getInt() == 0

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

