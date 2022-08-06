package toydb.lsm;

public class SparseIndexEntrySize {
    private int startOffset;
    private int endOffset;

    public SparseIndexEntrySize(int startOffset, int endOffset) {
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public int getStartOffset() {
        return startOffset;
    }

    public int getEndOffset() {
        return endOffset;
    }
}
