# 0001 client write through cache



```python
import binascii
import sys
import time
import traceback

#from redis import ConnectionError

from postmodel.exceptions import PostmodelCacheFailed
import json
from basepy.asynclog import logger

class CacheNode(object):
    def __init__(self, rds, check_interval=60):
        self.rds = rds
        self.status = 'up'  # 'down', 'starting', 'up'
        self.status_time = 0
        self.process_id = -1
        self.last_check_time = 0
        self.check_interval = check_interval
        self.start_cycle = 2

    def log_status(self):
        try:
            kwargs = self.rds.connection_pool.connection_kwargs
        except Exception:
            kwargs = {}
        node_desc = 'redis://%s:%s/%s' % \
                    (kwargs.get('host'), kwargs.get('port'), kwargs.get('db'))
        if self.status == 'down':
            logger.error('postmodel_cache_down', 'cache_node'=node_desc)
        logger.info(
            'cache node <%s> status changed to %s' % (node_desc, self.status))

    def check_node(self):
        try:
            self.last_check_time = time.time()
            info = self.rds.info()
            process_id = info.get('process_id')
            if self.process_id != -1 and process_id == self.process_id:
                return True
            else:
                self.process_id = process_id
        except Exception:
            return False
        return True

    def check_status(self):
        if self.status == 'down':
            if time.time() - self.last_check_time > self.check_interval:
                if self.check_node():
                    self.status_time = time.time()
                    self.status = 'starting'
                    self.log_status()
        elif self.status == 'starting':
            if time.time() - self.status_time > \
                    self.check_interval * self.start_cycle:
                self.status_time = time.time()
                self.status = 'up'
                self.log_status()
        elif self.status == 'up':
            if self.process_id == -1:
                if not self.check_node():
                    self.node_down(has_exception=False)

    def get_client(self):
        if self.status in ['starting', 'up']:
            return self.rds
        return None

    @property
    def writeonly(self):
        if self.status == 'starting':
            return True
        return False

    @property
    def enabled(self):
        if self.status in ['starting', 'up']:
            return True
        return False

    def node_down(self, has_exception=True):
        self.status_time = time.time()
        self.status = 'down'
        if has_exception:
            logger.error(sys.exc_info()[1])
            logger.error(traceback.format_exc())
        self.log_status()

    def mark_dirty_data(self, item_key):
        try:
            kwargs = self.rds.connection_pool.connection_kwargs
        except Exception:
            kwargs = {}
        node_desc = 'redis://%s:%s/%s' % \
                    (kwargs.get('host'), kwargs.get('port'), kwargs.get('db'))
        logger.error('POSTMODEL_DIRTYDATA', cache_node=node_desc, item_key=item_key)


class NullCache(object):
    def __init__(self, model_class):
        self.model_class = model_class

    def wrapper_hash_range_key(
            self, cache, hash_key_value, range_key_value=None):
        pass

    def get_writethrough(self, hash_key_value, range_key_value=None):
        return {}, -1, 0

    def set_writethrough(
            self, hash_key_value, range_key_value, item,
            orig_item, write_version):
        return True

    def delete_writethrough(self, hash_key_value, range_key_value=None):
        return True

    def mark_dirty(self, hash_key_value, range_key_value=None):
        return True


class RedynaCache(object):
    def __init__(self, model_class):
        self.model_class = model_class
        self.raise_on_fail = False

    def cache_instance(self, hash_key_value, range_key_value=None):
        sharding_key = self.get_sharding_key(hash_key_value, range_key_value)
        sharding_id = binascii.crc32(sharding_key.encode('utf-8')) % 100
        instance = cache_mamanger.cache_node(
            sharding_id, self.model_class.__name__)
        return instance

    def key_datatype(self, key):
        datatype = self.model_class.__schema__.get(key)
        if not datatype:
            return 's'

        # wait
        if datatype == str:
            return 's'
        elif datatype == bytes:
            return 'b'
        else:
            return 'n'

    def wrap_key_withtype(self, key):
        return '%s:%s' % (
            self.key_datatype(key), self.model_class.shortkey(key))

    @property
    def writethrough_expire(self):
        return self.model_class.__write_through_expire__


    def get_writethrough_key(self, hash_key_value, range_key_value=None):
        """
        the format of redis key:
        common = ${full_tablename}:${hash_key_value}:${range_key_value}
        write_through_key = ${common}:t
        """
        table_name = self.model_class.table_name()
        t_keys = [table_name, '%s' % hash_key_value]
        if range_key_value is not None:
            t_keys.append('%s' % range_key_value)
        t_keys.append('t')
        return ':'.join(t_keys)

    def get_sharding_key(self, hash_key_value, range_key_value=None):
        """
        the format of redis key:
        common = ${full_tablename}:${hash_key_value}:${range_key_value}
        sharding_key = ${common}
        """
        table_name = self.model_class.table_name()
        t_keys = [table_name, '%s' % hash_key_value]
        if range_key_value is not None:
            t_keys.append('%s' % range_key_value)
        return ':'.join(t_keys)

    def wrapper_hash_range_key(
            self, cache, hash_key_value, range_key_value=None):
        if not cache:
            return
        hashkey_name = self.model_class.hash_key_name()
        rangekey_name = self.model_class.range_key_name()
        cache[hashkey_name] = hash_key_value
        if rangekey_name and range_key_value:
            cache[rangekey_name] = range_key_value

    def get_writethrough(self, hash_key_value, range_key_value=None):
        cache = {}

        if not self.have_writethrough():
            return cache, 0, 0

        node = self.cache_instance(hash_key_value, range_key_value)
        if not node.enabled and self.raise_on_fail:
            raise PostmodelCacheFailed()
        if not node.enabled or node.writeonly:
            return {}, -1, 0

        rds = node.get_client()

        item_key = self.get_writethrough_key(hash_key_value, range_key_value)
        try:
            item_data = rds.hgetall(item_key)
            read_version = int(item_data.pop('_rv', -1))
            write_version = int(item_data.pop('_wv', 0))

            cache = dict(item_data)
        except ConnectionError:
            node.node_down()
            return {}, -1, 0

        return cache, read_version, write_version


    def set_writethrough(
            self, hash_key_value, range_key_value,
            item, orig_item, write_version):
        if not self.have_writethrough():
            return True

        node = self.cache_instance(hash_key_value, range_key_value)
        rds = node.get_client()
        item_key = self.get_writethrough_key(hash_key_value, range_key_value)

        if not node.enabled:
            node.mark_dirty_data(item_key)
            return True

        try:
            item_cache = dict(item)
            for k, v in item_cache.items():
                if isinstance(v, (set, frozenset)):
                    item_cache[k] = json.dumps(list(v))
            item_cache.update(_rv=write_version)
            deleted_keys = list(
                set(orig_item.keys()) - set(item_cache.keys()))
            pipe = rds.pipeline()
            if deleted_keys:
                pipe.hdel(item_key, *deleted_keys)
            pipe.hmset(item_key, item_cache)
            pipe.expire(item_key, self.writethrough_expire)
            pipe.execute()
        except ConnectionError:
            node.node_down()
            node.mark_dirty_data(item_key)
            return True

        return True

    def delete_writethrough(self, hash_key_value, range_key_value=None):
        node = self.cache_instance(hash_key_value, range_key_value)
        rds = node.get_client()
        item_key = self.get_writethrough_key(hash_key_value, range_key_value)

        if not node.enabled:
            node.mark_dirty_data(item_key)
            return True

        try:
            rds.delete(item_key)
        except ConnectionError:
            node.node_down()
            node.mark_dirty_data(item_key)
            return True

        return True

    def mark_dirty(self, hash_key_value, range_key_value=None):
        node = self.cache_instance(hash_key_value, range_key_value)
        rds = node.get_client()
        item_key = self.get_writethrough_key(hash_key_value, range_key_value)

        if not node.enabled:
            node.mark_dirty_data(item_key)
            return True

        try:
            pipe = rds.pipeline()
            pipe.hincrby(item_key, '_wv', 1)
            pipe.expire(item_key, self.writethrough_expire)
            pipe.execute()
        except ConnectionError:
            node.node_down()
            node.mark_dirty_data(item_key)
            return True

        return True
```