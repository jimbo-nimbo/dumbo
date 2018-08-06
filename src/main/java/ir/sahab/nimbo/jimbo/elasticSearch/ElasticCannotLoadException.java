package ir.sahab.nimbo.jimbo.elasticSearch;

public class ElasticCannotLoadException extends Exception {

    ElasticCannotLoadException()
    {
        super("Cannot connect to elasticsearch");
    }

}
