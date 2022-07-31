package toydb.lsm;

import toydb.common.StatusCode;
import toydb.lsm.IMemTable;

import java.util.concurrent.CompletableFuture;

public interface ISSTableWriter {
    CompletableFuture<StatusCode> submitWriteMemTableToDisk(SSTableMetaInformation sstableInfo);
}
