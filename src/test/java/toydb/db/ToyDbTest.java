package toydb.db;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import toydb.common.StatusCode;
import toydb.lsm.MemTableConcurrentSkipListMap;

import java.util.Arrays;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.*;

class ToyDbTest {
    private static ExecutorService pool = Executors.newFixedThreadPool(8);
    private static ExecutorCompletionService<Boolean> cs = new ExecutorCompletionService<>(pool);

    @Test
    void setAndGetOneValue_MemTableHashMapWithReadWriteLock() {
        ToyDbConfiguration config = ToyDbConfiguration.builder()
                .with(MemTableType.SkipList)
                .with(new Comparator<String>() {
                    @Override
                    public int compare(String o1, String o2) {
                        return o1.compareTo(o2);
                    }
                })
                .build();

        IToyDb toyDb = new ToyDb(config);

        String guid = UUID.randomUUID().toString();
        if (StatusCode.Ok != toyDb.set(guid, "aed269e1-1d53-458c-984d-cab622d208df_" + guid)) {
            Assertions.fail("Set failed");
        }

        if (toyDb.get(guid).getStatus() != StatusCode.Ok) {
            Assertions.fail("Get failed");
        };
    }

    @Test
    void setAndGetOneValue_MemTableConcurrentHashMap() {
        ToyDbConfiguration config = ToyDbConfiguration.builder()
                .with(MemTableType.SkipList)
                .with(new Comparator<String>() {
                    @Override
                    public int compare(String o1, String o2) {
                        return o1.compareTo(o2);
                    }
                })
                .build();

        IToyDb toyDb = new ToyDb(config);

        String guid = UUID.randomUUID().toString();
        if (StatusCode.Ok != toyDb.set(guid, "aed269e1-1d53-458c-984d-cab622d208df_" + guid)) {
            Assertions.fail("Set failed");
        }

        if (toyDb.get(guid).getStatus() != StatusCode.Ok) {
            Assertions.fail("Get failed");
        };
    }

    @Test
    void SSTableWriterTest() throws InterruptedException {
        ToyDbConfiguration config = ToyDbConfiguration.builder()
                .with(MemTableType.SkipList)
                .with(new Comparator<String>() {
                    @Override
                    public int compare(String o1, String o2) {
                        return o1.compareTo(o2);
                    }
                })
                .with(9)
                .build();

        IToyDb toyDb = new ToyDb(config);

        String[] keys = {"Joshi7", "Joshi6", "Joshi0", "Joshi1", "Joshi8", "Joshi2", "Joshi9", "Joshi3", "Joshi4", "Joshi5", "Joshi91", "Joshi92"};
        CountDownLatch countDownLatch = new CountDownLatch(13);
        Arrays.stream(keys).parallel().forEach(key -> {
                toyDb.set(key, "value-"+key);
                countDownLatch.countDown();
        });

        countDownLatch.await(4, TimeUnit.SECONDS);
    }
}