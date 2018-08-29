package ir.sahab.nimbo.jimbo.hbase;

public interface DuplicateChecker {
    boolean shouldFetch(String sourceUrl);
    void putMark(String sourceUrl);
}
