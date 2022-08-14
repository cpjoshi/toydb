package toydb.compaction;

import toydb.lsm.Value;

import java.util.*;

public class Compactor {
    private final PriorityQueue<CompactorEntry> compactorKeys;
    private final List<InputSSTable> inputSSTables;
    private final OutputSSTable outputSSTable;

    public Compactor(List<InputSSTable> inputSSTables) {
        compactorKeys = new PriorityQueue<>(CompactorEntry.GetComparator());
        this.inputSSTables = inputSSTables;
        this.outputSSTable = new OutputSSTable();
    }

    public void run() {
        outputSSTable.clear();
        primeInputs();

        do {
            CompactorEntry outputEntry = getNextEntryForOutput();
            writeEntryToOutput(outputEntry);
            List<CompactorEntry> entriesRemoved = removeObsoleteEntries(outputEntry);
            fillInputs(entriesRemoved);
        }
        while (havePendingEntries());
    }

    public OutputSSTable getOutputSSTable() {
        return outputSSTable;
    }

    private void primeInputs() {
        for (InputSSTable inputSSTable : inputSSTables) {
            addCompactorEntryFromInputFile(inputSSTable);
        }
    }

    private CompactorEntry getNextEntryForOutput() {
        CompactorEntry nextEntry = compactorKeys.remove();
        return nextEntry;
    }

    private void writeEntryToOutput(CompactorEntry nextEntry) {
        if (nextEntry.getValue().isDeleted()) {
            return;
        }

        outputSSTable.getOutputMemTable().set(nextEntry.getKey(), nextEntry.getValue().getVal().toString());
    }

    private List<CompactorEntry> removeObsoleteEntries(CompactorEntry outputEntry) {
        List<CompactorEntry> entriesRemoved = new ArrayList<>();
        entriesRemoved.add(outputEntry);

        CompactorEntry entry;
        boolean keyIdentifersEqual = false;

        do {
            entry = compactorKeys.peek();
            keyIdentifersEqual = (entry != null && entry.getKey().equals(outputEntry.getKey()));
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
            InputSSTable inputSSTable = inputSSTables.get(compactorEntry.getFileSequenceNumber());
            addCompactorEntryFromInputFile(inputSSTable);
        }
    }

    private boolean havePendingEntries() {
        return compactorKeys.size() > 0;
    }

    private void addCompactorEntryFromInputFile(InputSSTable inputSSTable) {
        if (inputSSTable.getInputFileIterator().hasNext()) {
            Map.Entry<String, Value<String>> inputEntry = inputSSTable.getInputFileIterator().next();
            CompactorEntry compactorEntry = new CompactorEntry(
                    inputSSTable.getFileSequenceNumber(),
                    inputEntry.getKey(),
                    inputEntry.getValue());

            compactorKeys.add(compactorEntry);
        }
    }
}
