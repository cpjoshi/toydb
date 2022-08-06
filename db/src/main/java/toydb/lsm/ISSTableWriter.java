package toydb.lsm;

import toydb.common.StatusCode;

import java.util.concurrent.CompletableFuture;

public interface ISSTableWriter {
    CompletableFuture<StatusCode> submitWriteMemTableToDisk(SSTableMetaInformation sstableInfo);
}
