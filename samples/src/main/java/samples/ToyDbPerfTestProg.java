package samples;

import toydb.common.StatusCode;
import toydb.db.IToyDb;
import toydb.db.MemTableType;
import toydb.db.ToyDb;
import toydb.db.ToyDbConfiguration;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

public class ToyDbPerfTestProg {
    private static ExecutorService pool = Executors.newFixedThreadPool(8);
    private static ExecutorCompletionService<Boolean> cs = new ExecutorCompletionService<>(pool);

    public static void main(String[] args) {
        ToyDbConfiguration config = ToyDbConfiguration.builder()
                .with(MemTableType.TreeMap)
                .withMemTableFlushThreshold(10000)
                .with(new Comparator<String>() {
                    @Override
                    public int compare(String o1, String o2) {
                        return o1.compareTo(o2);
                    }
                }).build();

        IToyDb toyDb = new ToyDb(config);
        testPerformance(toyDb, "MemTableHashMapWithReadWriteLock");
        toyDb.shutDown();

        config = ToyDbConfiguration.builder()
                .with(MemTableType.SkipList)
                .withMemTableFlushThreshold(10000)
                .with(new Comparator<String>() {
                    @Override
                    public int compare(String o1, String o2) {
                        return o1.compareTo(o2);
                    }
                }).build();

        IToyDb toyDb3 = new ToyDb(config);
        testPerformance(toyDb3, "MemTableConcurrentSkipListMap");
        toyDb3.shutDown();

        pool.shutdownNow();
    }

    private static void testPerformance(IToyDb toyDb, String logTag) {
        SecureRandom secureRandom = new SecureRandom();
        List<Future<Boolean>> lst = new ArrayList<>();
        int count = 9999;

        long startTime = System.currentTimeMillis();

        for (int i=0; i<count; i++) {
            lst.add(cs.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    String guid = UUID.randomUUID().toString();
                    if(StatusCode.Ok != toyDb.set(guid, "aed269e1-1d53-458c-984d-cab622d208df_" + guid)) {
                        return false;
                    }

                    if(secureRandom.nextInt() %2 == 0) {
                        if(toyDb.get(guid).getStatus() != StatusCode.Ok) {
                            return false;
                        };
                    }
                    return true;
                }
            }));
        }

        int doneSoFar = 0;
        boolean success = true;
        while (doneSoFar < count) {
            try {
                Boolean b = cs.take().get();
                ++doneSoFar;
                if(!b) {
                    success = false;
                    break;
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        System.out.printf("%s, status: %b, time_taken_ms: %d\n",
                logTag, success, System.currentTimeMillis() - startTime);
    }
}
