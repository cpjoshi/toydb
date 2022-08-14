package samples;

import toydb.common.Response;
import toydb.common.StatusCode;
import toydb.db.IToyDb;
import toydb.db.MemTableType;
import toydb.db.ToyDb;
import toydb.db.ToyDbConfiguration;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

public class ToyDbPerfTest {
    public static void main(String [] args) throws InterruptedException {
        ToyDbConfiguration config = ToyDbConfiguration.builder()
                .with(MemTableType.SkipList)
                .withMemTableFlushThreshold(10000)
                .with((o1, o2) -> o1.compareTo(o2))
                .withSparseIndexFactor(100)
                .build();

        IToyDb toyDb = new ToyDb(config);

        int totalKeys = 100000;
        String [] keys = new String[totalKeys];

        for (int i=0; i<totalKeys; i++) {
            keys[i] = UUID.randomUUID().toString();
        }

        int turn = 0;
        AtomicInteger totalKeysSaved = new AtomicInteger(0);
        Arrays.stream(keys).parallel().forEach(
                key -> {
                    toyDb.set(key, String.format("Value-%s-%d", key, turn));
                    totalKeysSaved.incrementAndGet();
                }
        );

        while (totalKeysSaved.get() < totalKeys) {
            Thread.currentThread().wait(500);
        }

        System.out.printf("Done saving keys....\n");

        ConcurrentLinkedQueue<Long> timings = new ConcurrentLinkedQueue<>();
        Arrays.stream(keys).parallel().forEach(
                key -> {
                    long startTime = System.nanoTime();
                    Response<String> response = toyDb.get(key);
                    timings.add(System.nanoTime() - startTime);
                    if(response.getStatus() != StatusCode.Ok) {
                        System.out.printf("Test Failed to get key - %s\n", key);
                    }
                }
        );

        while (timings.size() < totalKeys) {
            Thread.currentThread().wait(500);
        }

        LongStream ls = timings.stream().mapToLong(v -> v).sorted();
        long[] arr = ls.toArray();
        System.out.printf("Timing (nanoseconds): Min:%d Max:%d P95:%d P99:%d",
                arr[0],
                arr[totalKeys-1],
                arr[totalKeys/100 * 95 - 1],
                arr[totalKeys/100 * 99 - 1]);

        toyDb.shutDown();
    }
}
