package toydb.lsm;

import java.util.TreeMap;

public class SSTablesManager {
    private ISSTableWriter writer;
    private ISSTableReader ssTableReader;
    private TreeMap<Long, SSTableMetaInformation> sstables = new TreeMap<>((o1, o2) -> o2.compareTo(o1));

    public SSTablesManager(ISSTableWriter ssTableWriter, ISSTableReader ssTableReader) {
        this.writer = ssTableWriter;
        this.ssTableReader = ssTableReader;
    }

    public void flush(SSTableMetaInformation ssTableMetaInformation) {
        sstables.put(ssTableMetaInformation.getTimestamp(), ssTableMetaInformation);
        this.writer.submitWriteMemTableToDisk(ssTableMetaInformation);
    }

    public Value<String> get(String key) {
        //start searching from the most recent SSTable
        return null;
    }
}
