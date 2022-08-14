package toydb.compaction;

import toydb.lsm.ISSTableReader;
import toydb.lsm.Value;

import java.util.Iterator;
import java.util.Map;

public class InputSSTable {
    protected int fileSequenceNumber;
    protected Iterator<Map.Entry<String, Value<String>>> inputFileIterator;

    public InputSSTable(int fileSequenceNumber, Iterator<Map.Entry<String, Value<String>>> inputFileIterator) {
        this.fileSequenceNumber = fileSequenceNumber;
        this.inputFileIterator = inputFileIterator;
    }

    public int getFileSequenceNumber() {
        return fileSequenceNumber;
    }

    public Iterator<Map.Entry<String, Value<String>>> getInputFileIterator() {
        return inputFileIterator;
    }
}
