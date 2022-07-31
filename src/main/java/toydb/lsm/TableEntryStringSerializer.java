package toydb.lsm;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

public class TableEntryStringSerializer implements ITableEntrySerializer {
    @Override
    public void serialize(Map.Entry<String, Value> entry, DataOutputStream dataOutputStream) throws IOException {
        Value<String> valueWithStatus = entry.getValue();
        dataOutputStream.writeInt(entry.getKey().length());
        dataOutputStream.writeChars(entry.getKey());
        dataOutputStream.writeBoolean(valueWithStatus.isDeleted());
        dataOutputStream.writeInt(valueWithStatus.getVal().length());
        dataOutputStream.writeChars(valueWithStatus.getVal());
        dataOutputStream.writeLong(valueWithStatus.getLastCRUDOperationTimeStamp());
    }

    @Override
    public int entrySize(Map.Entry<String, Value> entry) {
        Value<String> valueWithStatus = entry.getValue();
        return Integer.BYTES +
                entry.getKey().length() +
                1 +
                Integer.BYTES +
                valueWithStatus.getVal().length() +
                Long.BYTES;
    }
}
