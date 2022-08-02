package toydb.lsm;

import toydb.common.Response;
import toydb.common.StatusCode;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemTableTreeMapWithReadWriteLock implements IMemTable {
    public static final String DELETED = "_DELETED_";
    private TreeMap<String, Value> kvMap = new TreeMap<>();
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    @Override
    public Response<String> get(String key) {
        try {
            readWriteLock.readLock().lock();
            if(!kvMap.containsKey(key)) {
                return new Response<>(StatusCode.NotFound);
            }

            Value<String> valueWithStatus = kvMap.get(key);
            return valueWithStatus.isDeleted() ?
                    new Response<String>(StatusCode.NotFound) :
                    new Response<String>(StatusCode.Ok, valueWithStatus.getVal());
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public StatusCode set(String key, String value) {
        try {
            readWriteLock.writeLock().lock();
            kvMap.put(key, new Value(value));
            return StatusCode.Ok;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public StatusCode delete(String key) {
        try {
            readWriteLock.writeLock().lock();
            kvMap.put(key, new Value(DELETED, true));
            return StatusCode.Ok;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public Set<Map.Entry<String, Value>> entrySet() {
        return kvMap.entrySet();
    }

    @Override
    public Boolean containsKey(String key) {
        return kvMap.containsKey(key);
    }

    @Override
    public long size() {
        return kvMap.size();
    }
}
