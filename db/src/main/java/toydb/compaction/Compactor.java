package toydb.compaction;

import java.util.*;

public class Compactor {
    private final List<String> inputFiles;
    private final List<String> mergedFiles;

    private PriorityQueue<CompactorEntry> compactorKeys;

    public Compactor(List<String> inputFiles) {
        this.inputFiles = inputFiles;
        mergedFiles = new LinkedList<>();
        compactorKeys = new PriorityQueue<>();
    }

    public List<String> getMergedFiles() {
        return mergedFiles;
    }

    public void run() {
        mergedFiles.clear();

        for (String file:inputFiles) {
        }
    }

}
