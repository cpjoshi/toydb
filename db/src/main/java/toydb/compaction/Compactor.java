package toydb.compaction;

import toydb.lsm.Value;

import java.util.*;

public class Compactor {
    private final List<String> mergedFiles;
    private final PriorityQueue<CompactorEntry> compactorKeys;
    private final List<CompactorInputFile> compactorInputFiles;

    public Compactor(List<String> inputFiles) {
        mergedFiles = new LinkedList<>();
        compactorKeys = new PriorityQueue<>(CompactorEntry.GetComparator());

        compactorInputFiles = new ArrayList<>();
        for (int i = 0; i < inputFiles.size(); i++) {
            CompactorInputFile compactorInputFile = new CompactorInputFile();
            compactorInputFile.fileSequenceNumber = i;
            compactorInputFile.inputFile = inputFiles.get(i);
            compactorInputFiles.add(compactorInputFile);
        }
    }

    public List<String> getMergedFiles() {
        return mergedFiles;
    }

    public void run() {
        try {
            mergedFiles.clear();
            openInputSSTables();
            primeInputs();

            do {
                CompactorEntry nextEntry = getNextEntry();
                writeEntryToOutput(nextEntry);
                List<CompactorEntry> entriesRemoved = clearObsoleteEntries(nextEntry);
                fillInputs(entriesRemoved);
            }
            while (havePendingEntries());
        } finally {
            closeInputSSTables();
        }
    }

    private boolean havePendingEntries() {
        return compactorKeys.size() > 0;
    }

    private void primeInputs() {
        for (CompactorInputFile compactorInputFile:compactorInputFiles) {
            addCompactorEntryFromInputFile(compactorInputFile);
        }
    }

    private void addCompactorEntryFromInputFile(CompactorInputFile compactorInputFile) {
        if (compactorInputFile.inputFileIterator.hasNext()) {
            Map.Entry<String, Value<String>> inputEntry = compactorInputFile.inputFileIterator.next();
            CompactorEntry compactorEntry = new CompactorEntry(
                    compactorInputFile.fileSequenceNumber,
                    inputEntry.getKey(),
                    inputEntry.getValue());

            compactorKeys.add(compactorEntry);
        }
    }

    private CompactorEntry getNextEntry() {
        CompactorEntry nextEntry = compactorKeys.remove();
        return nextEntry;
    }

    private void writeEntryToOutput(CompactorEntry nextEntry) {
        // TODO:
    }

    private List<CompactorEntry> clearObsoleteEntries(CompactorEntry nextEntry) {
        List<CompactorEntry> entriesRemoved = new ArrayList<>();
        entriesRemoved.add(nextEntry);

        CompactorEntry entry;
        boolean keyIdentifersEqual = false;

        do {
            entry = compactorKeys.peek();
            keyIdentifersEqual = (entry != null && entry.getKey().equals(nextEntry.getKey()));
            if (keyIdentifersEqual) {
                CompactorEntry removedEntry = compactorKeys.remove();
                entriesRemoved.add(removedEntry);
            }
        }
        while (keyIdentifersEqual);

        return entriesRemoved;
    }

    private void fillInputs(List<CompactorEntry> entriesRemoved) {
        for (CompactorEntry compactorEntry: entriesRemoved) {
            CompactorInputFile compactorInputFile = compactorInputFiles.get(compactorEntry.getFileSequenceNumber());
            addCompactorEntryFromInputFile(compactorInputFile);
        }
    }

    private void openInputSSTables() {
    }

    private void closeInputSSTables() {
    }

    private class CompactorInputFile {
        private int fileSequenceNumber;
        private String inputFile;
        private Iterator<Map.Entry<String, Value<String>>> inputFileIterator;
    }
}
