package toydb.db;

import toydb.common.Response;
import toydb.common.StatusCode;

import java.util.List;
import java.util.Map;

public interface IToyDb {
    Response<String> get(String key);
    StatusCode set(String key, String value);
    StatusCode delete(String key);
    Response<Map<String, String>> batchGet(List<String> keys);
    void shutDown();
}
