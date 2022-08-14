package toydb.lsm;

import toydb.common.StatusCode;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SSTableWriter implements ISSTableWriter {
    private final ITableEntrySerializer serializer;
    private int sparseIndexFactor = 4;
    ExecutorService executorService = Executors.newSingleThreadExecutor();

    public SSTableWriter(ITableEntrySerializer serializer, int sparseIndexFactor)
    {
        this.serializer = serializer;
        this.sparseIndexFactor = sparseIndexFactor;
    }

    @Override
    public CompletableFuture<StatusCode> submitWriteMemTableToDisk(SSTableMetaInformation sstableMetaData) {
        CompletableFuture<StatusCode> completableFuture = new CompletableFuture<>();
        executorService.submit(() -> {
            try {
                StatusCode code = flush(sstableMetaData);
                completableFuture.complete(code);
            } catch (IOException e) {
                e.printStackTrace();
                completableFuture.completeExceptionally(e);
            }
        });

        return completableFuture;
    }

    public StatusCode flush(SSTableMetaInformation sstableMetaData) throws IOException {
        IMemTable memTable = sstableMetaData.getMemTable();
        try (
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        FileOutputStream fos = new FileOutputStream(sstableMetaData.getSsTableFilePath().toString(), true)
                    ) {

            //TODO:
            // 1. write this in the byte buffer chunks.
            // 2. Chunks can be independently compressed and encrypted.
            int entryNumber = 0;
            for (Map.Entry<String, Value> entry : memTable.entrySet()) {
                int entryStartOffset = out.size();
                this.serializer.serialize(entry, out);
                int entryEndOffset = out.size();

                if (entryNumber==0 || entryNumber == memTable.size()-1 || (entryNumber % sparseIndexFactor) == 0) {
                    sstableMetaData.getSstableSparseIndex().put(entry.getKey(),
                            new SparseIndexEntrySize(entryStartOffset, entryEndOffset));
                }
                entryNumber++;
            }

            baos.writeTo(fos);

            //TODO:
            // Write the SSTable index file
            // Write BloomFilter file
            // compress and encrypt SSTable chunks

            //for (Map.Entry<String, Integer> indexEntry: sstableMetaData.getSstableSparseIndex().entrySet()
            //     ) {
            //    System.out.printf("%s-%d\n", indexEntry.getKey(), indexEntry.getValue());
            //}
            return StatusCode.Ok;
        } finally {
            sstableMetaData.setMemTable(null);
        }
    }
}
