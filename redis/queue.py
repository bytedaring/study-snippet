#!/use/bin/env python3

# 加入队列
def send_sold_email_via_queue(conn, seller, item, price, buyer):
    data = {
        'seller_id': seller,
        'item_id': item,
        'price': price,
        'buyer_id': buyer,
        'time': time.time()
    }
    conn.rpush('queue:email', json.dumps(data))


# 从队列取出邮件发送
def process_sold_mail_queue(conn):
    while not QUIT:
        packed = conn.blpop(['queue:email'], 30)
        if not packed:
            continue

        to_send = json.loads(packed[1])
        try:
            fetch_data_and_send_sold_email(to_send)
        except EmailSendError as serr:
            log_error('Failed to send sold email', err, to_send)
        else:
            log_success('Sent sold email', to_send)


# 多个可执行任务
# 一个队列可执行多种不同类型任务
def worker_watch_queue(conn, queue, callbacks):
    while not QUIT:
        packed = conn.blpop([queue], 30)
        if not packed:
            continue

        name, args = json.loads(packed[1])
        if name not in callbacks:
            log_error("Unknown callbacks %s" % name)
            continue
        callbacks[name](*args)


# 优先级队列, queues 优先级队列集和
def worker_watch_queues(conn, queues, callbacks):
    while not QUIT:
        # blpop 支持优先级队列
        packed = conn.blpop(queues, 30)
        if not packed:
            continue

        name, args = json.loads(packed[1])
        if name not in callbacks:
            log_error("Unknown callbacks %s" % name)
            continue
        callbacks[name](*args)


# 将任务添加到有序集合中，待执行时间作为分值
# 外加一个进程检查需要执行的任务，进其加入到执行任务队列中
# queue,name,args, identifier: 处理任务队列名，处理任务的回掉函数名，回掉函数参数，任务唯一标识
# 无需延迟时，直接加入任务队列中
def execute_later(conn, queue, name, args, delay=0):
    identifier = str(uuid.uuid4())
    item = json.dumps([identifier, queue, name, args])
    if delay > 0:
        conn.zadd('delayed:', item, time.time() + delay)
    else:
        conn.rpush('queue:' + queue, item)
    return identifier


# 投递任务到队列
def poll_queue():
    while not QUIT:
        # 获取延迟集和中第一个任务
        item = conn.zrange('delayed:', 0, 0, withscores=True)
        if not item or item[0][1] > time.time():
            # 任务队列没有任务，或者任务执行时间还没有到
            time.sleep(.01)
            continue

        item = item[0][0]
        identifier, queue, name, args = json.loads(item)

        # 为移动任务，尝试获取锁
        locked = acquire_lock(conn, identifier)
        if not locked:
            continue

        # 将任务放入到队列中
        if conn.zrem('delayed:', item):
            conn.rpush('queue:' + queue, item)

        # 释放锁
        release_lock(conn, identifier, locked)
