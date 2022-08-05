package toydb.lsm;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TableEntryBinarySerializer implements ITableEntrySerializer {
    @Override
    public void serialize(Map.Entry<String, Value> entry, DataOutputStream dataOutputStream) throws IOException {
        Value<String> valueWithStatus = entry.getValue();
        dataOutputStream.writeInt(entry.getKey().length());
        byte[] arr = entry.getKey().getBytes(StandardCharsets.UTF_8);
        dataOutputStream.write(arr);
        dataOutputStream.writeBoolean(valueWithStatus.isDeleted());
        dataOutputStream.writeInt(valueWithStatus.getVal().length());
        arr = valueWithStatus.getVal().getBytes(StandardCharsets.UTF_8);
        dataOutputStream.write(arr);
        dataOutputStream.writeLong(valueWithStatus.getLastCRUDOperationTimeStamp());
    }

    @Override
    public Map<String, Value<String>> deserialize(ByteBuffer buf) {
        HashMap<String, Value<String>> hashMap = new HashMap<>();
        while (buf.position() < buf.limit()) {
            Map.Entry<String, Value<String>> entry = readNext(buf);
            if (entry != null)
                hashMap.put(entry.getKey(), entry.getValue());
        }
        return hashMap;
    }

    public Map.Entry<String, Value<String>> readNext(ByteBuffer buf) {
        try {
            int keySize = buf.getInt();
            byte[] arr = new byte[keySize];
            Arrays.fill(arr, (byte) 0);
            buf.get(arr, 0, keySize);
            String key = new String(arr, StandardCharsets.UTF_8);
            boolean isDeleted = buf.get() != 0;
            int valueSize = buf.getInt();
            arr = new byte[valueSize];
            Arrays.fill(arr, (byte) 0);
            buf.get(arr, 0, valueSize);
            String value = new String(arr, StandardCharsets.UTF_8);
            long lastCRUDOperationTimeStamp = buf.getLong();
            return new AbstractMap.SimpleEntry<String, Value<String>>(key,
                    new Value<String>(value, isDeleted, lastCRUDOperationTimeStamp));
        } catch (BufferUnderflowException ex) {
            // TODO: this file is corrupted. what to do about this SSTable file?
            ex.printStackTrace();
        }
        return null;
    }

    public Map.Entry<String, Value<String>> readNext(RandomAccessFile file) {
        try {
            int keySize = file.readInt();
            byte[] arr = new byte[keySize];
            Arrays.fill(arr, (byte) 0);
            file.read(arr, 0, keySize);
            String key = new String(arr, StandardCharsets.UTF_8);
            boolean isDeleted = file.read() != 0;
            int valueSize = file.readInt();
            arr = new byte[valueSize];
            Arrays.fill(arr, (byte) 0);
            file.read(arr, 0, valueSize);
            String value = new String(arr, StandardCharsets.UTF_8);
            long lastCRUDOperationTimeStamp = file.readLong();
            return new AbstractMap.SimpleEntry<String, Value<String>>(key,
                    new Value<String>(value, isDeleted, lastCRUDOperationTimeStamp));
        } catch (IOException ex) {
            // TODO: this file is corrupted. what to do about this SSTable file?
            ex.printStackTrace();
        }
        return null;
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
