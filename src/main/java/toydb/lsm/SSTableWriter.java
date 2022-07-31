package toydb.lsm;

import com.sun.source.tree.Tree;
import toydb.common.StatusCode;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SSTableWriter implements ISSTableWriter {
    private final ITableEntrySerializer serializer;
    private String baseDir;
    ExecutorService executorService = Executors.newSingleThreadExecutor();

    public SSTableWriter(ITableEntrySerializer serializer, String baseDir) {
        this.serializer = serializer;
        this.baseDir = baseDir;
    }

    @Override
    public CompletableFuture<StatusCode> submitWriteMemTableToDisk(SSTableMetaInformation sstableMetaData) {
        CompletableFuture<StatusCode> completableFuture = new CompletableFuture<>();
        executorService.submit(() -> {
            try (
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    DataOutputStream out = new DataOutputStream(baos);
                    FileOutputStream fos = new FileOutputStream(Paths.get(this.baseDir, sstableMetaData.getSstableFileName()).toString(),
                            true)
                    ) {

                //TODO:
                // 1. write this in the byte buffer chunks.
                // 2. Chunks can be independently compressed and encrypted.
                int entryNumber = 0;
                IMemTable memTable = sstableMetaData.getMemTable();
                for (Map.Entry<String, Value> entry : memTable.entrySet()) {
                    //TODO: sparse index
                    if (entryNumber==0 || entryNumber == memTable.size()-1) {
                        sstableMetaData.getSstableSparseIndex().put(entry.getKey(), out.size());
                    }
                    this.serializer.serialize(entry, out);
                    entryNumber++;
                }

                baos.writeTo(fos);
                //persist to file complete, release memtable
                sstableMetaData.setMemTable(null);

                //TODO:
                // Write the SSTable index file
                // Write BloomFilter file
                // compress and encrypt SSTable chunks

                //for (Map.Entry<String, Integer> indexEntry: sstableMetaData.getSstableSparseIndex().entrySet()
                //     ) {
                //    System.out.printf("%s-%d\n", indexEntry.getKey(), indexEntry.getValue());
                //}
            } catch (IOException e) {
                e.printStackTrace();
                completableFuture.completeExceptionally(e);
            }
            completableFuture.complete(StatusCode.Ok);
        });

        return completableFuture;
    }
}
