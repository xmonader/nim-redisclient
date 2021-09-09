import ../redisclient, asyncdispatch, times
from os import sleep
from net import Port

## A fork of zedeus's redpool for redisclient
## https://github.com/zedeus/redpool

type
  ConnectionError* = object of IOError

  RedisConn[T: Redis|AsyncRedis] = ref object
    active: bool
    conn: T

  RedisPool*[T: Redis|AsyncRedis] = ref object
    conns: seq[RedisConn[T]]
    host: string
    port: Port
    db: int
    maxConns: int
    noConnectivityTest: bool
    isBlocking: bool
    waitTimeout: float

const
  WAIT_TIME_IN_MILISECONDS = 10

template raiseException(x, msg) = raise newException(x, msg)

proc newAsyncRedisConn(pool: RedisPool[AsyncRedis]): Future[RedisConn[AsyncRedis]] {.async.} =
  result = RedisConn[AsyncRedis](
    conn: await openAsync(pool.host, pool.port),
    active: false
  )
  discard await result.conn.select(pool.db)

proc newRedisConn(pool: RedisPool[Redis]): RedisConn[Redis] =
  result = RedisConn[Redis](
    conn: open(pool.host, pool.port),
    active: false
  )
  discard result.conn.select(pool.db)

template newPoolImpl(x) =
  result = RedisPool[x](
    host: host,
    port: Port(port),
    db: db,
    maxConns: maxConns,
    noConnectivityTest: noConnectivityTest,
    isBlocking: isBlocking,
    waitTimeout: waitTimeout
  )
  for n in 0 ..< initSize:
    when x is Redis:
      var conn = newRedisConn(result)
    else:
      var conn = await newAsyncRedisConn(result)
    result.conns.add(conn)

proc newAsyncRedisPool*(initSize: int, maxConns=10, host="localhost", port=6379, db=0, noConnectivityTest=false, isBlocking=true, waitTimeout=10.0): Future[RedisPool[AsyncRedis]] {.async.} =
  ## Create new AsyncRedis pool
  newPoolImpl(AsyncRedis)

proc newRedisPool*(initSize: int, maxConns=10, host="localhost", port=6379, db=0, noConnectivityTest=false, isBlocking=true, waitTimeout=10.0): RedisPool[Redis] =
  ## Create new Redis pool
  newPoolImpl(Redis)

template acquireImpl(x) =
  let now = epochTime()
  while true:
    for i, rconn in pool.conns:
      if not rconn.active:
        if unlikely(not pool.noConnectivityTest):
          # Try to send PING to server to test for connection
          try:
            when x is Redis:
              discard rconn.conn.ping()
            else:
              discard await rconn.conn.ping()
          except:
            pool.conns.del(i)
            break

        rconn.active = true
        result = rconn.conn
        break

    if result != nil:
      # break while loop
      break

    # All connections are busy, and no more slot to make new connection
    if unlikely(pool.conns.len >= pool.maxConns):
      # If `isBlocking` is set, wait for a connection till waitTimeout exceed
      if pool.isBlocking and epochTime() - now < pool.waitTimeout:
        when x is Redis:
          sleep(WAIT_TIME_IN_MILISECONDS)
        else:
          await sleepAsync(WAIT_TIME_IN_MILISECONDS)
        continue
      # Raise exception if not blocking, or blocking timed out
      raiseException(ConnectionError, "No connection available.")

    # Still having free slot, making new connection
    when x is Redis:
      let newConn = newRedisConn(pool)
    else:
      let newConn = await newAsyncRedisConn(pool)
    newConn.active = true
    pool.conns.add newConn
    result = newConn.conn

proc acquire*(pool: RedisPool[AsyncRedis]): Future[AsyncRedis] {.async.} =
  ## Acquires AsyncRedis connection from pool
  acquireImpl(AsyncRedis)

proc acquire*(pool: RedisPool[Redis]): Redis =
  ## Acquires Redis connection from pool
  acquireImpl(Redis)

proc release*[T: Redis|AsyncRedis](pool: RedisPool[T], conn: T) =
  ## Returns connection to pool
  for rconn in pool.conns:
    if rconn.conn == conn:
      rconn.active = false
      break

proc close(pool: RedisPool[AsyncRedis]) {.async.} =
  ## Close all connections
  for rconn in pool.conns:
    rconn.active = false
    discard await rconn.conn.quit()

proc close(pool: RedisPool[Redis]) =
  ## Close all connections
  for rconn in pool.conns:
    rconn.active = false
    discard rconn.conn.quit()


template withAcquire*[T: Redis|AsyncRedis](pool: RedisPool[T], conn, body: untyped) =
  ## Automatically acquire and release a connection
  when T is Redis:
    let `conn` {.inject.} = pool.acquire()
  else:
    let `conn` {.inject.} = await pool.acquire()
  try:
    body
  finally:
    pool.release(`conn`)


when isMainModule:
  proc main {.async.} =
    let pool = await newAsyncRedisPool(1)
    let conn = await pool.acquire()
    echo await conn.ping()
    pool.release(conn)

    pool.withAcquire(conn2):
      echo await conn2.ping()
    await pool.close()

  proc sync =
    let pool = newRedisPool(1)
    let conn = pool.acquire()
    echo conn.ping()
    pool.release(conn)

    pool.withAcquire(conn2):
      echo conn2.ping()
    pool.close()

  proc timeout =
    let pool = newRedisPool(3, maxConns=5, waitTimeout=1)
    for i in 0..6:
      let conn = pool.acquire()
      echo conn.ping()
    pool.close()

  proc closed =
    # test for connection closed befor acquire
    # restart redis server for example
    let pool = newRedisPool(3, maxConns=5, noConnectivityTest=true)
    echo "Restart Redis server, and press Enter to continue.."
    discard stdin.readLine
    for i in 0..5:
      pool.withAcquire(conn):
        echo conn.ping()
    pool.close()

  waitFor main()
  sync()
  closed()
  timeout()





