package toydb.compaction;

import toydb.common.StatusCode;
import toydb.lsm.IMemTable;
import toydb.lsm.Value;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.junit.jupiter.api.Assertions.*;

class CompactorTest {

    @Test
    public void sanity() {
        // Key count in input tables are different
        // Keys must be returned in order
        // Last key wins

        ConcurrentSkipListMap<String, Value<String>> ssTable1 = new ConcurrentSkipListMap<>();
        ssTable1.put("key1", new Value<String>("value.T1.Key1"));
        ssTable1.put("key2", new Value<String>("value.T1.Key2"));
        ssTable1.put("key5", new Value<String>("value.T1.Key5"));
        ssTable1.put("key4", new Value<String>("value.T1.Key4"));

        ConcurrentSkipListMap<String, Value<String>> ssTable2 = new ConcurrentSkipListMap<>();
        ssTable2.put("key3", new Value<String>("value.T2.Key3"));
        ssTable2.put("key1", new Value<String>("value.T2.Key1"));

        InputSSTable input1 = new InputSSTable(0, ssTable1.entrySet().iterator());
        InputSSTable input2 = new InputSSTable(1, ssTable2.entrySet().iterator());

        ArrayList<InputSSTable> inputs = new ArrayList<>();
        inputs.add(input1);
        inputs.add(input2);

        Compactor compactor = new Compactor(inputs);
        compactor.run();

        IMemTable compactedTable = compactor.getOutputSSTable().getOutputMemTable();
        assertEquals(5, compactedTable.size());

        assertEquals("value.T2.Key1", compactedTable.get("key1").getResult());
        assertEquals("value.T1.Key2", compactedTable.get("key2").getResult());
        assertEquals("value.T2.Key3", compactedTable.get("key3").getResult());
        assertEquals("value.T1.Key4", compactedTable.get("key4").getResult());
        assertEquals("value.T1.Key5", compactedTable.get("key5").getResult());
    }

    @Test
    public void keyNotPresentInOutputIfLastOccurrenceIsDeleted() {

        ConcurrentSkipListMap<String, Value<String>> ssTable1 = new ConcurrentSkipListMap<>();
        ssTable1.put("key1", new Value<String>("value.T1.Key1"));
        ssTable1.put("key2", new Value<String>("value.T1.Key2"));
        ssTable1.put("key5", new Value<String>("value.T1.Key5"));
        ssTable1.put("key4", new Value<String>("value.T1.Key4"));

        ConcurrentSkipListMap<String, Value<String>> ssTable2 = new ConcurrentSkipListMap<>();
        ssTable2.put("key3", new Value<String>("value.T2.Key3"));
        ssTable2.put("key1", new Value<String>("value.T2.Key1"));
        ssTable2.put("key4", new Value<String>("value.T1.Key4", true));

        InputSSTable input1 = new InputSSTable(0, ssTable1.entrySet().iterator());
        InputSSTable input2 = new InputSSTable(1, ssTable2.entrySet().iterator());

        ArrayList<InputSSTable> inputs = new ArrayList<>();
        inputs.add(input1);
        inputs.add(input2);

        Compactor compactor = new Compactor(inputs);
        compactor.run();

        IMemTable compactedTable = compactor.getOutputSSTable().getOutputMemTable();
        assertEquals(4, compactedTable.size());

        assertEquals("value.T2.Key1", compactedTable.get("key1").getResult());
        assertEquals("value.T1.Key2", compactedTable.get("key2").getResult());
        assertEquals("value.T2.Key3", compactedTable.get("key3").getResult());
        assertEquals("value.T1.Key5", compactedTable.get("key5").getResult());

        assertEquals(StatusCode.NotFound, compactedTable.get("key4").getStatus());
        assertNull(compactedTable.get("key4").getResult());
    }

    @Test
    public void keyPresentInOutputIfIntermediateOccurrenceIsDeleted() {

        ConcurrentSkipListMap<String, Value<String>> ssTable1 = new ConcurrentSkipListMap<>();
        ssTable1.put("key1", new Value<String>("value.T1.Key1"));
        ssTable1.put("key2", new Value<String>("value.T1.Key2"));
        ssTable1.put("key5", new Value<String>("value.T1.Key5"));
        ssTable1.put("key4", new Value<String>("value.T1.Key4", true));

        ConcurrentSkipListMap<String, Value<String>> ssTable2 = new ConcurrentSkipListMap<>();
        ssTable2.put("key3", new Value<String>("value.T2.Key3"));
        ssTable2.put("key1", new Value<String>("value.T2.Key1"));
        ssTable2.put("key4", new Value<String>("value.T2.Key4"));

        InputSSTable input1 = new InputSSTable(0, ssTable1.entrySet().iterator());
        InputSSTable input2 = new InputSSTable(1, ssTable2.entrySet().iterator());

        ArrayList<InputSSTable> inputs = new ArrayList<>();
        inputs.add(input1);
        inputs.add(input2);

        Compactor compactor = new Compactor(inputs);
        compactor.run();

        IMemTable compactedTable = compactor.getOutputSSTable().getOutputMemTable();
        assertEquals(5, compactedTable.size());

        assertEquals("value.T2.Key1", compactedTable.get("key1").getResult());
        assertEquals("value.T1.Key2", compactedTable.get("key2").getResult());
        assertEquals("value.T2.Key3", compactedTable.get("key3").getResult());
        assertEquals("value.T2.Key4", compactedTable.get("key4").getResult());
        assertEquals("value.T1.Key5", compactedTable.get("key5").getResult());
    }
}