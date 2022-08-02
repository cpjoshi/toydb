package toydb.lsm;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import toydb.common.Response;
import toydb.common.StatusCode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Stack;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

class SSTablesManagerTest {
    static private Path testsPath = Paths.get("./toydbTestFiles");

    @BeforeEach
    void setUp() throws IOException {
        if(!Files.exists(testsPath)) {
            Files.createDirectory(testsPath);
        }
    }

    @Test
    @Order(1)
    void testGetSSTablesFlushed() throws IOException, InterruptedException, ExecutionException {
        SSTablesManager tablesManager = new SSTablesManager(new SSTableWriter(new TableEntryBinarySerializer()),
                new SSTableReader(new TableEntryBinarySerializer()));


        String [] keys = new String[10];
        for(int i=0; i<=9; i++) {
            keys[i] = String.format("Joshi%d", i);
        }

        int value_recency_1 = 1;
        IMemTable memTable1 = new MemTableConcurrentSkipListMap();
        Arrays.stream(keys).forEach(k -> memTable1.set(k, String.format("value-%s-%d", k, value_recency_1)));
        SSTableMetaInformation ssTableMetaInformation1 = new SSTableMetaInformation(memTable1, testsPath.toString());
        CompletableFuture<StatusCode> flushed2 = tablesManager.flush(ssTableMetaInformation1);
        //we wait till the file is flushed to disk.
        StatusCode code = flushed2.get();
        Assertions.assertEquals(StatusCode.Ok, code);

        int value_recency_2 = 2;
        IMemTable memTable2 = new MemTableConcurrentSkipListMap();
        Arrays.stream(keys).forEach(k -> memTable2.set(k, String.format("value-%s-%d", k, value_recency_2)));
        SSTableMetaInformation ssTableMetaInformation2 = new SSTableMetaInformation(memTable2, testsPath.toString());
        CompletableFuture<StatusCode> flushed1 = tablesManager.flush(ssTableMetaInformation2);
        //we wait till the file is flushed to disk.
        code = flushed1.get();
        Assertions.assertEquals(StatusCode.Ok, code);

        Response<String> value = tablesManager.get("Joshi1");
        Assertions.assertEquals("value-Joshi1-2", value.getResult());

        //cleanup
        Files.deleteIfExists(ssTableMetaInformation1.getSsTableFilePath());
        Files.deleteIfExists(ssTableMetaInformation2.getSsTableFilePath());
    }

    @Test
    @Order(2)
    void testGetSSTablesMayNotBeFlushed() throws IOException, InterruptedException, ExecutionException {
        SSTablesManager tablesManager = new SSTablesManager(new SSTableWriter(new TableEntryBinarySerializer()),
                new SSTableReader(new TableEntryBinarySerializer()));


        String [] keys = new String[10];
        for(int i=0; i<=9; i++) {
            keys[i] = String.format("Joshi%d", i);
        }

        int value_recency_1 = 1;
        IMemTable memTable1 = new MemTableConcurrentSkipListMap();
        Arrays.stream(keys).forEach(k -> memTable1.set(k, String.format("value-%s-%d", k, value_recency_1)));
        SSTableMetaInformation ssTableMetaInformation1 = new SSTableMetaInformation(memTable1, testsPath.toString());
        tablesManager.flush(ssTableMetaInformation1);

        int value_recency_2 = 2;
        IMemTable memTable2 = new MemTableConcurrentSkipListMap();
        Arrays.stream(keys).forEach(k -> memTable2.set(k, String.format("value-%s-%d", k, value_recency_2)));
        SSTableMetaInformation ssTableMetaInformation2 = new SSTableMetaInformation(memTable2, testsPath.toString());
        tablesManager.flush(ssTableMetaInformation2);

        Response<String> value = tablesManager.get("Joshi1");
        Assertions.assertEquals("value-Joshi1-2", value.getResult());

        //cleanup
        Files.deleteIfExists(ssTableMetaInformation1.getSsTableFilePath());
        Files.deleteIfExists(ssTableMetaInformation2.getSsTableFilePath());
    }

}