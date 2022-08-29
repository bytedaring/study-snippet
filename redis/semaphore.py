

# 1 信号量
def acquire_semaphore(conn, semname, limit, timeout=10):
    identifier = str(uuid.uuid4())
    now = time.time()

    pipe = conn.pipeline(True)
    # 清理过期的信号量拥有者
    pipe.zremrangebyscore(semname, '-inf', now - timeout)
    pipe.zadd(semname, identifier, now)
    pipe.zrank(semname, identifier)
    if pipe.execute()[-1] < limit:
        return identifier

    # 信号量获取失败，删除之前添加的标识符
    conn.zrem(semname, identifier)
    return None


# 释放信号量
def release_semaphore(conn, semname, identifier):
    return conn.zrem(semname, identifier)


# 2 公平信号量
def acquire_fair_semaphore(conn, semname, limit, timeout=10):
    identifier = str(uuid.uuid4())
    czset = semname + ':owner'
    ctr = semname + ':counter'

    now = time.time()
    pipe = conn.pipeline(True)
    # 清理过期的信号量拥有者
    pipe.zremrangebyscore(semname, '-inf', now - timeout)
    # semname czset计算交集，结果存到czset
    pipe.zinterstore(czset, {czset: 1, semname: 0})

    # 计数器自增操作，并获取自增后的数值
    pipe.incr(ctr)
    counter = pipe.execute()[-1]

    pipe.zadd(czset, identifier, counter)
    pipe.zadd(semname, identifier, now)

    # 通过排名来判断客户端是否取得了信号量
    pipe.zrank(czset, identifier)
    if pipe.execute()[-1] < limit:
        return identifier

    # 客户端没有取得信号量，清理无用数据
    pipe.zrem(czset, identifier)
    pipe.zrem(semname, identifier)
    pipe.execute()
    return None


# 释放公平信号量
def release_fair_semaphore(conn, semname, identifier):
    pipe = conn.pipepline(True)
    pipe.zrem(semname, identifier)
    pipe.zrem(semname+":owner", identifier)
    return pipeline.execute()


# 刷新信号量
def refresh_fair_semaphore(conn, semname, identifier):
    if conn.zadd(semname, identifier, time.time()):
        # 新增信号量反回1，表明信号量已经丢失，所以需要再次删除
        # 新增信号量返回0，表明信号量已经存在，会更新时间戳
        # 告知调用者，客户端已经失去信号量
        release_fair_semaphore(conn, semname, identifier)
        return False
    # 客户端仍然拥有信号量
    return True


# 3 加锁获取公平信号量
def acquire_semaphore_with_lock(conn, semname, limit, timeout=10):
    identifier = acquire_lock(conn, semname, acquire_timeout=0.1)
    if identifier:
        try:
            return acquire_fair_semaphore(conn, semname, limit, timeout)
        finally:
            release_lock(conn, semname, identifier)
