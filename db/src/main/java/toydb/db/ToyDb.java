package toydb.db;

import toydb.common.Response;
import toydb.common.StatusCode;
import toydb.lsm.*;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ToyDb implements IToyDb {
    private Comparator<String> keyComparator;
    private AtomicReference<IMemTable> memTable;
    private ToyDbConfiguration configuration;
    private ISSTableWriter sstableWriter;
    private ISSTableReader ssTableReader;
    private SSTablesManager ssTablesManager;
    private ScheduledExecutorService memTablePeriodicFlushService;

    public ToyDb(ToyDbConfiguration configuration) {
        if (configuration == null) {
            throw new NullPointerException("Configuration can't be null.");
        }
        this.configuration = configuration;
        this.memTable = new AtomicReference<>(getNewMemTable(configuration));
        this.keyComparator = configuration.getKeyComparator();
        this.sstableWriter = new SSTableWriter(new TableEntryBinarySerializer(), configuration.getSparseIndexFactor());
        this.ssTableReader = new SSTableReader(new TableEntryBinarySerializer());
        ssTablesManager = new SSTablesManager(this.sstableWriter, this.ssTableReader);

        memTablePeriodicFlushService = Executors.newScheduledThreadPool(1);
        memTablePeriodicFlushService.scheduleAtFixedRate(
                () -> flushMemTableToDisk(configuration), 0, 2, TimeUnit.SECONDS);
    }

    public ToyDb(Comparator<String> keyComparator) {
        this.keyComparator = keyComparator;
    }

    @Override
    public Response<String> get(String key) {
        IMemTable table = memTable.get();
        if(table.containsKey(key)) {
            return table.get(key);
        }

        return ssTablesManager.get(key);
    }

    @Override
    public StatusCode set(String key, String value) {
        IMemTable table = memTable.get();
        StatusCode statusCode = table.set(key, value);
        if (table.size() % configuration.getMemtableFlushThreshold() == 0) {
            memTablePeriodicFlushService.submit(() -> flushMemTableToDisk(configuration));
        }
        return statusCode;
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

    @Override
    public void shutDown() {
        memTablePeriodicFlushService.shutdown();
    }

    private IMemTable getNewMemTable(ToyDbConfiguration configuration) {
        return (configuration.getMemTableType() == MemTableType.SkipList)
                ? new MemTableConcurrentSkipListMap() : new MemTableTreeMapWithReadWriteLock();
    }

    private void flushMemTableToDisk(ToyDbConfiguration configuration) {
        IMemTable table = memTable.get();
        if (table.size() >= configuration.getMemtableFlushThreshold()) {
            System.out.printf("DiskIO memtable-size: %d\n", table.size());
            ssTablesManager.flush(new SSTableMetaInformation(table, configuration.getBaseDir()));
            memTable.getAndSet(getNewMemTable(configuration));
        }
    }
}
