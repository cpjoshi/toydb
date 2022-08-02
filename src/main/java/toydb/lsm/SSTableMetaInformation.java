package toydb.lsm;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

public class SSTableMetaInformation {
    private String sstableFileName;
    private String sstableIndexFileName;
    private TreeMap<String, SparseIndexEntrySize> sstableSparseIndex;
    private long timestamp;
    private IMemTable memTable;
    private String baseDir;
    private AtomicInteger memTableRefernece = new AtomicInteger(0);

    public SSTableMetaInformation(IMemTable memTable, String baseDir) {
        timestamp = System.nanoTime();
        sstableFileName = String.format("SST.%d.dat", timestamp);
        sstableIndexFileName = String.format("SSTIndex.%d.dat", timestamp);
        sstableSparseIndex = new TreeMap<String, SparseIndexEntrySize>();
        this.memTable = memTable;
        this.baseDir = baseDir;
    }

    public String getSstableFileName() {
        return sstableFileName;
    }

    public String getSstableIndexFileName() {
        return sstableIndexFileName;
    }

    public Path getSsTableFilePath() {
        return Paths.get(this.baseDir, sstableFileName);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public TreeMap<String, SparseIndexEntrySize> getSstableSparseIndex() {
        return sstableSparseIndex;
    }

    public IMemTable acquireMemtableReference() {
        memTableRefernece.incrementAndGet();
        return memTable;
    }

    public void releaseMemtableReference() {
        int reference = memTableRefernece.decrementAndGet();
        if(reference <= 0) {
            memTable = null;
        }
    }

    public void setMemTable(IMemTable memTable) {
        this.memTable = memTable;
    }
}
