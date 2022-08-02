package toydb.lsm;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Map;

public interface ITableEntrySerializer {
    void serialize(Map.Entry<String, Value> entry, DataOutputStream dataOutputStream) throws IOException;
    Map<String, Value<String>> deserialize(ByteBuffer buf);
    int entrySize(Map.Entry<String, Value> entry);
}
