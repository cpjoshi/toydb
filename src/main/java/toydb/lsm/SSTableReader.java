package toydb.lsm;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

public class SSTableReader implements ISSTableReader {
    private final ITableEntrySerializer serializer;

    public SSTableReader(ITableEntrySerializer serializer) {
        if (serializer == null)
            throw new NullPointerException("serializer is required for Table Reader");
        this.serializer = serializer;
    }

    public Map<String, Value<String>> get(String key, SSTableMetaInformation sstableInfo) {
        HashMap<String, Value<String>> map = new HashMap<>();
        Map.Entry<String, SparseIndexEntrySize> lowerBoundEntry =  sstableInfo.getSstableSparseIndex().floorEntry(key);
        Map.Entry<String, SparseIndexEntrySize> upperBoundEntry =  sstableInfo.getSstableSparseIndex().ceilingEntry(key);

        int startPosition = 0, endPosition = 0;
        if (lowerBoundEntry == null) {
            startPosition = upperBoundEntry == null ? 0 : upperBoundEntry.getValue().getStartOffset();
            endPosition = upperBoundEntry == null ? 0 : upperBoundEntry.getValue().getEndOffset();
        } else if (upperBoundEntry == null) {
            startPosition = lowerBoundEntry == null ? 0 : lowerBoundEntry.getValue().getStartOffset();
            endPosition = lowerBoundEntry == null ? 0 : lowerBoundEntry.getValue().getEndOffset();
        } else {
            startPosition = lowerBoundEntry.getValue().getStartOffset();
            endPosition = upperBoundEntry.getValue().getEndOffset();
        }

        if(startPosition == endPosition || startPosition > endPosition)
            return map;

        try (
                RandomAccessFile file = new RandomAccessFile(sstableInfo.getSsTableFilePath().toString(), "r");
        ) {
            MappedByteBuffer buf = file.getChannel().map(FileChannel.MapMode.READ_ONLY, startPosition, endPosition-startPosition);
            return this.serializer.deserialize(buf);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return map;
    }
}
