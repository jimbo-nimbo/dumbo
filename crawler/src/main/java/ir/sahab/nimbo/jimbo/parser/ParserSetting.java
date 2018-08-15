package ir.sahab.nimbo.jimbo.parser;

public class ParserSetting {
    private final int parserThreadCount;

    public ParserSetting(int parserThreadCount) {
        this.parserThreadCount = parserThreadCount;
    }

    public int getParserThreadCount() {
        return parserThreadCount;
    }
}
