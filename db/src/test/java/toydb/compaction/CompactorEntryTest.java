package toydb.compaction;

import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.PriorityQueue;

import static org.junit.jupiter.api.Assertions.*;

class CompactorEntryTest {
    @Test
    void higherFileSequenceNumberIsEarlierInOrder() {
        CompactorEntry c1 = new CompactorEntry("aKey", 2);
        CompactorEntry c2 = new CompactorEntry("aKey", 3);
        CompactorEntry c3 = new CompactorEntry("bKey", 3);

        Comparator<CompactorEntry> comparator = CompactorEntry.GetComparator();

        assertTrue(comparator.compare(c1, c2) > 0);
        assertTrue(comparator.compare(c3, c2) > 0);
        assertTrue(comparator.compare(c2, c1) < 0);
    }

    @Test
    void equalityTests() {
        CompactorEntry c1 = new CompactorEntry("aKey", 2);
        CompactorEntry c2 = new CompactorEntry("aKey", 2);
        CompactorEntry c3 = new CompactorEntry("bKey", 2);

        assertTrue(c1.equals(c2));
        assertFalse(c1.equals(null));
        assertFalse(c1.equals(c3));
        assertFalse(c1.equals(new Object()));
    }

    @Test
    void priorityQueueAndComparatorTest() {
        PriorityQueue<CompactorEntry> priorityQueue = new PriorityQueue<CompactorEntry>(CompactorEntry.GetComparator());

        CompactorEntry c1 = new CompactorEntry("aKey", 2);
        CompactorEntry c2 = new CompactorEntry("aKey", 3);
        CompactorEntry c3 = new CompactorEntry("bKey", 3);

        priorityQueue.add(c1);
        priorityQueue.add(c2);
        priorityQueue.add(c3);

        CompactorEntry retrieved = priorityQueue.poll();
        assertTrue(retrieved.equals(c2));

        retrieved = priorityQueue.poll();
        assertTrue(retrieved.equals(c1));

        retrieved = priorityQueue.poll();
        assertTrue(retrieved.equals(c3));
    }
}