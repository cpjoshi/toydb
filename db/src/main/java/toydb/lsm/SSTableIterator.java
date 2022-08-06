package toydb.lsm;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Map;

public class SSTableIterator implements Iterator<Map.Entry<String, Value<String>>>, Closeable {
    private RandomAccessFile randomAccessFile;
    private FileChannel fc;
    private TableEntryBinarySerializer serializer =new TableEntryBinarySerializer();

    public SSTableIterator(String sstableFile) throws FileNotFoundException {
        randomAccessFile = new RandomAccessFile(sstableFile, "r");
        fc = randomAccessFile.getChannel();
    }

    @Override
    public boolean hasNext() {
        try {
            return fc.position() < fc.size();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public Map.Entry<String, Value<String>> next() {
        return serializer.readNext(randomAccessFile);
    }

    @Override
    public void close() throws IOException {
        fc.close();
        randomAccessFile.close();
    }
}
