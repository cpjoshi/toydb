package toydb.lsm;

import java.util.TreeMap;

public class SSTableMetaInformation {
    private String sstableFileName;
    private String sstableIndexFileName;
    private TreeMap<String, Integer> sstableSparseIndex;
    private long timestamp;
    private IMemTable memTable;

    public SSTableMetaInformation(IMemTable memTable) {
        timestamp = System.currentTimeMillis();
        sstableFileName = String.format("SST.%d.dat", timestamp);
        sstableIndexFileName = String.format("SSTIndex.%d.dat", timestamp);
        sstableSparseIndex = new TreeMap<String, Integer>();
        this.memTable = memTable;
    }

    public String getSstableFileName() {
        return sstableFileName;
    }

    public String getSstableIndexFileName() {
        return sstableIndexFileName;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public TreeMap<String, Integer> getSstableSparseIndex() {
        return sstableSparseIndex;
    }

    public IMemTable getMemTable() {
        return memTable;
    }

    public void setMemTable(IMemTable memTable) {
        this.memTable = memTable;
    }
}
