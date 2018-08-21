package ir.sahab.nimbo.jimbo.hbase;

public interface DuplicateChecker {
    boolean existMark(String sourceUrl);
    void putMark(String sourceUrl, String value);
}
