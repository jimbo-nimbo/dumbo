package ir.sahab.nimbo.jimbo.hbase;

public interface DuplicateChecker {
    public boolean existMark(String sourceUrl);
    public void putMark(String sourceUrl, String value);
}
