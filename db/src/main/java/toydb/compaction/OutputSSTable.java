package toydb.compaction;

import toydb.lsm.IMemTable;
import toydb.lsm.MemTableConcurrentSkipListMap;

import java.util.ArrayList;
import java.util.List;

public class OutputSSTable {
    private List<String> ssTables;
    private IMemTable outputMemTable;

    public OutputSSTable() {
        this.ssTables = new ArrayList<>();
        this.outputMemTable = new MemTableConcurrentSkipListMap();
    }

    public IMemTable getOutputMemTable() {
        return outputMemTable;
    }

    public void clear() {
        ssTables.clear();
        outputMemTable = new MemTableConcurrentSkipListMap();
    }
}
