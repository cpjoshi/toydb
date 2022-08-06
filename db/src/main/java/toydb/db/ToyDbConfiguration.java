package toydb.db;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;

public class ToyDbConfiguration {
    private Comparator<String> keyComparator;
    private int memtableFlushThreshold = 100;
    private MemTableType memTableType = MemTableType.SkipList;
    private String baseDir = "./toydbfiles";

    public Comparator<String> getKeyComparator() {
        return keyComparator;
    }

    public static ToyDbConfigurationBuilder builder() {
        return new ToyDbConfigurationBuilder();
    }

    public int getMemtableFlushThreshold() {
        return memtableFlushThreshold;
    }

    public MemTableType getMemTableType() {
        return memTableType;
    }

    public String getBaseDir() {
        return baseDir;
    }

    public static class ToyDbConfigurationBuilder {
        private Comparator<String> keyComparator;
        private int memtableFlushThreshold = 100;
        private MemTableType memTableType;
        private String baseDir = "./toydbfiles";

        public ToyDbConfiguration build() {
            if(!Files.exists(Paths.get(baseDir))) {
                try {
                    Files.createDirectory(Paths.get(baseDir));
                } catch (IOException e) {
                    e.printStackTrace();
                    return null;
                }
            }
            ToyDbConfiguration instance = new ToyDbConfiguration();
            instance.keyComparator = this.keyComparator;
            instance.memtableFlushThreshold = this.memtableFlushThreshold;
            instance.memTableType = this.memTableType;
            instance.baseDir = this.baseDir;
            return instance;
        }
        
        public ToyDbConfigurationBuilder with(Comparator<String> keyComparator) {
            this.keyComparator = keyComparator;
            return this;
        }

        public ToyDbConfigurationBuilder withMemTableFlushThreshold(int memtableFlushThreshold) {
            this.memtableFlushThreshold = memtableFlushThreshold;
            return this;
        }

        public ToyDbConfigurationBuilder with(MemTableType memTableType) {
            this.memTableType = memTableType;
            return this;
        }

        public ToyDbConfigurationBuilder with(String baseDir) {
            this.baseDir = baseDir;
            return this;
        }
    }
}
