#!/user/bin/env python3
import bisect
import redis

valid_characters = '<abcdefghijklmnopqrstuvwxyz>'


def find_prefix_range(prefix):
    posn = bisect.bisect_left(valid_characters, prefix[-1:])
    suffix = valid_characters[(posn or 1) - 1]
    return prefix[:-1] + suffix + '>', prefix + '<'


def autocomplete_on_prefix(conn, guild, prefix):
    start, end = find_prefix_range(prefix)
    indentifier = str(uuid.uuid4())
    start += indentifier
    end += indentifier
    zset_name = 'members:' + guid

    conn.zadd(zset_name, start, 0, end, 0)
    pipeline = conn.pipeline(True)
    while True:
        try:
            pipeline.watch(zset_name)
            sindex = pipeline.zrank(zset_name, start)
            eindex = pipeline.zrank(zset_name, end)
            erange = min(sindex + 9, eindex - 2)
            pipeline.multi()

            pipeline.zrem(zset_name, start, end)
            pipeline.zrange(zset_name, sindex, erange)
            items = pipeline.execute()[-1]
            break
        except redis.execptions.WatchError:
            continue
    return [item for item in items if '<' not in item]
