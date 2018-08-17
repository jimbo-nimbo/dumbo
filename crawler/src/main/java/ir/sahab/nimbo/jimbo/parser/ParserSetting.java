package ir.sahab.nimbo.jimbo.parser;

import ir.sahab.nimbo.jimbo.main.Setting;

public class ParserSetting extends Setting {
    private final int parserThreadCount;

    public ParserSetting() {
        super("parser");
        parserThreadCount = Integer.parseInt(properties.getProperty("parser_thread_count"));
    }

    public ParserSetting(int parserThreadCount) {
        super("parser");
        this.parserThreadCount = parserThreadCount;
    }

    public int getParserThreadCount() {
        return parserThreadCount;
    }
}
