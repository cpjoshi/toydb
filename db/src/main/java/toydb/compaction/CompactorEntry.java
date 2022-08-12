package toydb.compaction;

import java.util.Comparator;
import java.util.Objects;

class CompactorEntry {
    private final String key;
    private final int fileSequenceNumber;

    public CompactorEntry(String key, int fileSequenceNumber) {
        this.key = key;
        this.fileSequenceNumber = fileSequenceNumber;
    }

    public String getKey() {
        return key;
    }

    public int getFileSequenceNumber() {
        return fileSequenceNumber;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj.getClass() != this.getClass()) {
            return false;
        }

        CompactorEntry o2 = (CompactorEntry)obj;

        Comparator<CompactorEntry> comparator = GetComparator();
        return comparator.compare(this, o2) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, fileSequenceNumber);
    }

    public static Comparator<CompactorEntry> GetComparator() {
        Comparator<CompactorEntry> comparator = new Comparator<CompactorEntry>() {
            @Override
            public int compare(CompactorEntry o1, CompactorEntry o2) {

                // Returns a negative integer, zero, or a positive integer
                //  as the first argument is less than, equal to, or greater than the second.

                if (o1 == null && o2 == null) {
                    return 0;
                }

                if (o1 == null || o2 == null) {
                    return o1 == null ? -1 : 1;
                }

                int keyCompare = o1.key.compareTo(o2.key);
                if (keyCompare == 0) {
                    keyCompare = o2.fileSequenceNumber - o1.fileSequenceNumber;
                }

                return keyCompare;
            }
        };

        return comparator;
    }
}
