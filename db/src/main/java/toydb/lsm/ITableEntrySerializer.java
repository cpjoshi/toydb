package toydb.lsm;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public interface ITableEntrySerializer {
    void serialize(Map.Entry<String, Value> entry, DataOutputStream dataOutputStream) throws IOException;
    Map<String, Value<String>> deserialize(ByteBuffer buf);
    int entrySize(Map.Entry<String, Value> entry);
}
