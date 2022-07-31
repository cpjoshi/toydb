package toydb.db;

import toydb.common.Response;
import toydb.common.StatusCode;
import toydb.lsm.*;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ToyDb implements IToyDb {
    private Comparator<String> keyComparator;
    private AtomicReference<IMemTable> memTable;
    private ToyDbConfiguration configuration;
    private ISSTableWriter sstableWriter;
    private SSTablesManager ssTablesManager;

    public ToyDb(ToyDbConfiguration configuration) {
        if (configuration == null) {
            throw new NullPointerException("Configuration can't be null.");
        }
        this.configuration = configuration;
        this.memTable = new AtomicReference<>(getNewMemTable(configuration));
        this.keyComparator = configuration.getKeyComparator();
        this.sstableWriter = new SSTableWriter(new TableEntryStringSerializer(),
                configuration.getBaseDir());
        ssTablesManager = new SSTablesManager(this.sstableWriter);

        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(
                () -> flushMemTableToDisk(configuration), 0, 2, TimeUnit.SECONDS);
    }

    public ToyDb(Comparator<String> keyComparator) {
        this.keyComparator = keyComparator;
    }

    @Override
    public Response<String> get(String key) {
        IMemTable table = memTable.get();
        return table.get(key);
    }

    @Override
    public StatusCode set(String key, String value) {
        IMemTable table = memTable.get();
        return table.set(key, value);
    }

    @Override
    public StatusCode delete(String key) {
        IMemTable table = memTable.get();
        return table.delete(key);
    }

    @Override
    public Response<Map<String, String>> batchGet(List<String> keys) {
        return null;
    }

    private IMemTable getNewMemTable(ToyDbConfiguration configuration) {
        return (configuration.getMemTableType() == MemTableType.SkipList)
                ? new MemTableConcurrentSkipListMap() : new MemTableTreeMapWithReadWriteLock();
    }

    private void flushMemTableToDisk(ToyDbConfiguration configuration) {
        IMemTable table = memTable.get();
        if (table.size() >= configuration.getMemtableFlushThreshold()) {
            System.out.printf("DiskIO memtable-size: %d\n", table.size());
            IMemTable tmpTable = memTable.getAndSet(getNewMemTable(configuration));
            ssTablesManager.flush(new SSTableMetaInformation(tmpTable));
        }
    }
}