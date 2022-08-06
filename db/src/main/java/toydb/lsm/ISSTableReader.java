package toydb.lsm;

import java.util.Map;

public interface ISSTableReader {
    Map<String, Value<String>> get(String key, SSTableMetaInformation sstableInfo);
}
