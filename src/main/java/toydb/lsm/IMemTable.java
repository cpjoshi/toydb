package toydb.lsm;

import toydb.common.Response;
import toydb.common.StatusCode;

import java.util.Map;
import java.util.Set;

public interface IMemTable {
    Response<String> get(String key);
    StatusCode set(String key, String value);
    StatusCode delete(String key);
    Set<Map.Entry<String, Value>> entrySet();
    long size();
}
