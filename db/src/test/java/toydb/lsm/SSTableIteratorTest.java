package toydb.lsm;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import toydb.common.StatusCode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

class SSTableIteratorTest {
    static private Path testsPath = Paths.get("./toydbTestFiles");

    @BeforeEach
    void setUp() throws IOException {
        if(!Files.exists(testsPath)) {
            Files.createDirectory(testsPath);
        }
    }

    @Test
    void testReadIterator() throws IOException {
        IMemTable memTable = new MemTableConcurrentSkipListMap();

        String [] keys = new String[10];
        for(int i=0; i<=9; i++) {
            keys[i] = String.format("Joshi%d", i);
        }

        Arrays.stream(keys).forEach(k -> memTable.set(k, String.format("value-%s", k)));
        SSTableWriter writer = new SSTableWriter(new TableEntryBinarySerializer());
        SSTableMetaInformation ssTableMetaInformation = new SSTableMetaInformation(memTable, testsPath.toString());
        writer.flush(ssTableMetaInformation);

        //Iterator should read total 10 records
        int recordsRead = 0;

        try (SSTableIterator ssTableIterator = new SSTableIterator(ssTableMetaInformation.getSsTableFilePath().toString())) {
            while (ssTableIterator.hasNext()) {
                ssTableIterator.next();
                recordsRead++;
            }
        }

        Assertions.assertEquals(10, recordsRead);
    }
}