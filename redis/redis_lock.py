#!/user/bin/env python3

import bisect
import redis
import time


def release_lock(conn, lockname, identifier):
    pip = conn.pipeline(True)
    lockname = 'lock:' + lockname

    while True:
        try:
            pipe.watch(lockname)
            if pipe.get(lockname) == identifier:
                pipe.multi()
                pipe.delete(lockname)
                pipe.execute()
                return True

            pipe.unwatch()
            break
        except redis.exceptions.WatchError:
            pass

    return False


def acquire_lock(conn, lockname, acquire_timeout=10):
    identifier = str(uuid.uuid4())
    end = time.time() + acquire_timeout
    while time.time() < end:
        # 获取锁
        if conn.setnx('lock' + lockname, identifier):
            return identifier

        time.sleep(.001)
    return False


def acquire_lock_with_timeout(conn, lockname, acquire_timeout=10,
                              lock_timeout=10):
    identifier = str(uuid.uuid4())

    lock_timeout = int(math.ceil(lock_timeout))  # 确保传递给EXPIRE的都是整数
    end = time.time() + acquire_timeout
    while time.time() < end:
        # 获取锁并设置过期时间
        if conn.setnx('lock' + lockname, identifier):
            conn.expire(lockname, lock_timeout)
            return identifier
        elif not conn.ttl(lockname):
            # 检查过期时间，并在有需要时对其进行更新
            conn.expire(lockname, lock_timeout)

        time.sleep(.001)
    return False


# 使用锁，来购买商品
def purchase_item_with_lock(conn, buyerid, itemid, sellerid):
    buyer = "users:%s" % buyerid
    seller = "seller:%s" % sellerid
    item = "%s.%s" % (itemid, sellerid)
    inventory = "inventory:%s" % buyerid

    locked = acquire_lock(conn, marker)
    if not locked:
        return False

    pipe = conn.multi()
    try:
        pipe.zscore("marker:", item)
        pipe.hget(buyer, 'funds')
        price, funds = pipe.execute()
        if price is None or price > funds:
            return None

        pipe.hincrby(seller, 'funds', int(price))
        pipe.hincrby(buyer, 'funds', int(-price))
        pipe.sadd(inventory, itemid)
        pipe.zrem("market:", item)

        pipe.execute()
        return True
    finally:
        release_lock(conn, market, locked)
