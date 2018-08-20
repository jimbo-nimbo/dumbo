package ir.sahab.nimbo.jimbo.elasticsearch;

public class ElasticCannotLoadException extends Exception {

    ElasticCannotLoadException()
    {
        super("Cannot connect to elasticsearch");
    }

}
