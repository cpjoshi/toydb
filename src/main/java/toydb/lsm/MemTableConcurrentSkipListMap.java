package toydb.lsm;

import toydb.common.Response;
import toydb.common.StatusCode;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemTableConcurrentSkipListMap implements IMemTable {
    public static final String DELETED = "_DELETED_";
    ConcurrentSkipListMap<String, Value> kvMap = new ConcurrentSkipListMap<>();
    @Override
    public Response<String> get(String key) {
        if(!kvMap.containsKey(key)) {
            return new Response<>(StatusCode.NotFound);
        }

        Value<String> valueWithStatus = kvMap.get(key);
        return valueWithStatus.isDeleted() ?
                new Response<String>(StatusCode.NotFound) :
                new Response<String>(StatusCode.Ok, valueWithStatus.getVal());
    }

    @Override
    public StatusCode set(String key, String value) {
        kvMap.put(key, new Value(value));
        return StatusCode.Ok;
    }

    @Override
    public StatusCode delete(String key) {
        kvMap.put(key, new Value(DELETED, true));
        return StatusCode.Ok;
    }

    @Override
    public Set<Map.Entry<String, Value>> entrySet() {
        return kvMap.entrySet();
    }

    @Override
    public long size() {
        return kvMap.size();
    }
}
