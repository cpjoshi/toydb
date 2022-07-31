package toydb.lsm;

public class Value <T> {
    private final boolean deleted;
    private final T val;
    private long lastCRUDOperationTimeStamp;

    public Value(T val) {
        this.val = val;
        deleted = false;
        lastCRUDOperationTimeStamp = System.currentTimeMillis();
    }

    public Value(T val, boolean deleted) {
        this.val = val;
        this.deleted = deleted;
        lastCRUDOperationTimeStamp = System.currentTimeMillis();
    }

    public boolean isDeleted() {
        return deleted;
    }

    public T getVal() {
        return val;
    }

    public long getLastCRUDOperationTimeStamp() {
        return lastCRUDOperationTimeStamp;
    }
}
