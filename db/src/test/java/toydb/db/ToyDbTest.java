package toydb.db;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import toydb.common.Response;
import toydb.common.StatusCode;

import java.util.Arrays;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.*;

class ToyDbTest {
    private static ExecutorService pool = Executors.newFixedThreadPool(8);
    private static ExecutorCompletionService<Boolean> cs = new ExecutorCompletionService<>(pool);

    @Test
    @Order(1)
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

        Assertions.assertTrue(config.getMemtableFlushThreshold() > 0);
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
    @Order(2)
    void getFromPersistedSSTableTest() throws InterruptedException {
        ToyDbConfiguration config = ToyDbConfiguration.builder()
                .with(MemTableType.SkipList)
                .with((o1, o2) -> o1.compareTo(o2))
                .withMemTableFlushThreshold(9)
                .build();

        IToyDb toyDb = new ToyDb(config);

        String[] keys = {"Joshi07", "Joshi06", "Joshi00", "Joshi01", "Joshi08", "Joshi02", "Joshi09", "Joshi03", "Joshi04", "Joshi05", "Joshi10", "Joshi11"};
        CountDownLatch waiter = new CountDownLatch(1);
        Arrays.stream(keys).parallel().forEach(key -> {
            toyDb.set(key, "value-" + key);
        });

        //wait for 3 seconds for Memtable to be Flushed
        waiter.await(3, TimeUnit.SECONDS);


        String[] keys2 = {"Joshi14", "Joshi18", "Joshi15", "Joshi19", "Joshi16", "Joshi23", "Joshi17", "Joshi20", "Joshi22", "Joshi21", "Joshi13", "Joshi12"};
        Arrays.stream(keys2).parallel().forEach(key -> {
            toyDb.set(key, "value-" + key);
        });

        //wait for 3 seconds for Memtable to be Flushed
        waiter.await(3, TimeUnit.SECONDS);

        //test get value from second SSTable
        Response<String> response = toyDb.get("Joshi19");
        Assertions.assertEquals(StatusCode.Ok, response.getStatus());
        Assertions.assertEquals(response.getResult(), "value-Joshi19");

        //test get value from first SSTable
        response = toyDb.get("Joshi00");
        Assertions.assertEquals(StatusCode.Ok, response.getStatus());
        Assertions.assertEquals(response.getResult(), "value-Joshi00");

    }

    @Test
    @Order(3)
    void getFromMayNotBePersistedSSTableTest() throws InterruptedException {
        ToyDbConfiguration config = ToyDbConfiguration.builder()
                .with(MemTableType.SkipList)
                .with((o1, o2) -> o1.compareTo(o2))
                .withMemTableFlushThreshold(9)
                .build();

        IToyDb toyDb = new ToyDb(config);

        String[] keys = {"Joshi07", "Joshi06", "Joshi00", "Joshi01", "Joshi08", "Joshi02", "Joshi09", "Joshi03", "Joshi04", "Joshi05", "Joshi10", "Joshi11"};
        Arrays.stream(keys).parallel().forEach(key -> {
            toyDb.set(key, "value-" + key);
        });


        String[] keys2 = {"Joshi14", "Joshi18", "Joshi15", "Joshi19", "Joshi16", "Joshi23", "Joshi17", "Joshi20", "Joshi22", "Joshi21", "Joshi13", "Joshi12"};
        Arrays.stream(keys2).parallel().forEach(key -> {
            toyDb.set(key, "value-" + key);
        });

        //test get value from second SSTable
        Response<String> response = toyDb.get("Joshi19");
        Assertions.assertEquals(StatusCode.Ok, response.getStatus(), "Joshi19 was not found");
        Assertions.assertEquals(response.getResult(), "value-Joshi19");

        //test get value from first SSTable
        response = toyDb.get("Joshi00");
        Assertions.assertEquals(StatusCode.Ok, response.getStatus(), "Joshi00 was not found");
        Assertions.assertEquals(response.getResult(), "value-Joshi00");
    }
}