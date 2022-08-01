package toydb.lsm;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.MappedByteBuffer;
import java.util.Map;

public interface ITableEntrySerializer {
    void serialize(Map.Entry<String, Value> entry, DataOutputStream dataOutputStream) throws IOException;
    Map<String, Value<String>> deserialize(MappedByteBuffer buf);
    int entrySize(Map.Entry<String, Value> entry);
}
