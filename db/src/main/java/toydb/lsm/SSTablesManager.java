package toydb.lsm;

import toydb.common.Response;
import toydb.common.StatusCode;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;

public class SSTablesManager {
    private ISSTableWriter writer;
    private ISSTableReader ssTableReader;
    private ConcurrentSkipListMap<Long, SSTableMetaInformation> sstables =
            new ConcurrentSkipListMap<>((o1, o2) -> o2.compareTo(o1));

    public SSTablesManager(ISSTableWriter ssTableWriter, ISSTableReader ssTableReader) {
        this.writer = ssTableWriter;
        this.ssTableReader = ssTableReader;
    }

    public CompletableFuture<StatusCode> flush(SSTableMetaInformation ssTableMetaInformation) {
        if (sstables.containsKey(ssTableMetaInformation.getTimestamp())) {
            //this happened due to two SSTables getting created in same nanosecond.
            System.out.printf("Found two memtables with same timestamp. Fixing it...\n");
            ssTableMetaInformation.setTimestamp(ssTableMetaInformation.getTimestamp()+1);
        }
        sstables.put(ssTableMetaInformation.getTimestamp(), ssTableMetaInformation);
        return this.writer.submitWriteMemTableToDisk(ssTableMetaInformation);
    }

    public Response<String> get(String key) {
        // scan all SSTables from latest to oldest
        for (Map.Entry<Long, SSTableMetaInformation> sstable : sstables.entrySet()) {
            SSTableMetaInformation ssTableMetaInformation = sstable.getValue();

            // it's possible that SSTable is in the process of being written to the disk.
            // For this case we will acquire the memtable reference and check in memtable
            IMemTable memTable = ssTableMetaInformation.getMemTable();
            if (memTable == null) {
                // this SSTable is flushed to disk, read from the SSTable
                Map<String, Value<String>> map = ssTableReader.get(key, ssTableMetaInformation);
                if (map.containsKey(key)) {
                    return map.get(key).isDeleted() ?
                            new Response<>(StatusCode.NotFound) :
                            new Response<>(StatusCode.Ok, map.get(key).getVal());
                }
            } else if (memTable.containsKey(key)) {
                return memTable.get(key);
            }
        }
        return new Response<>(StatusCode.NotFound);
    }
}
