package toydb.lsm;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SSTableWriterTest {

    static private Path testsPath = Paths.get("./toydbTestFiles");

    @BeforeAll
    static void setup() throws IOException {
        if(!Files.exists(testsPath)) {
            Files.createDirectory(testsPath);
        }
    }

    @Test
    void testReadFlushedSSTable() throws IOException {
        IMemTable memTable = new MemTableConcurrentSkipListMap();

        String [] keys = new String[10];
        for(int i=0; i<=9; i++) {
            keys[i] = String.format("Joshi%d", i);
        }

        Arrays.stream(keys).forEach(k -> memTable.set(k, String.format("value-%s", k)));
        SSTableWriter writer = new SSTableWriter(new TableEntryBinarySerializer());
        SSTableMetaInformation ssTableMetaInformation = new SSTableMetaInformation(memTable, testsPath.toString());
        writer.flush(ssTableMetaInformation);

        SSTableReader ssTableReader = new SSTableReader(new TableEntryBinarySerializer());
        HashMap<String, Value<String>> map = (HashMap<String, Value<String>>) ssTableReader.get("Joshi1", ssTableMetaInformation);
        Assertions.assertEquals("value-Joshi1", map.get("Joshi1").getVal());

        map = (HashMap<String, Value<String>>) ssTableReader.get("ajsdhu1u90n", ssTableMetaInformation);
        Assertions.assertEquals(null, map.get("ajsdhu1u90n"));

        map = (HashMap<String, Value<String>>) ssTableReader.get("Joshi", ssTableMetaInformation);
        Assertions.assertEquals(null, map.get("Joshi"));

        Arrays.stream(keys).forEach(k -> {
            Assertions.assertEquals(String.format("value-%s", k), ssTableReader.get(k, ssTableMetaInformation).get(k).getVal());
        });

        //cleanup
        Files.deleteIfExists(ssTableMetaInformation.getSsTableFilePath());
    }
}