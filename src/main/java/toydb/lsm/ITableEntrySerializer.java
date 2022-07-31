package toydb.lsm;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

public interface ITableEntrySerializer {
    void serialize(Map.Entry<String, Value> entry, DataOutputStream dataOutputStream) throws IOException;
    int entrySize(Map.Entry<String, Value> entry);
}
