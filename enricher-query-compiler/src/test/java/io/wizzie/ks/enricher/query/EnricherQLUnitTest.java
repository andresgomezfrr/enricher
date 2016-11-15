package io.wizzie.ks.enricher.query;

import io.wizzie.ks.enricher.query.compiler.EnricherQLLexer;
import io.wizzie.ks.enricher.query.compiler.EnricherQLParser;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EnricherQLUnitTest {

    @Test
    public void InsertIntoShouldWork() {

        String query = "INSERT INTO STREAM rb_output";

        ANTLRInputStream inputStream = new ANTLRInputStream(query);
        EnricherQLLexer lexer = new EnricherQLLexer(inputStream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        EnricherQLParser parser = new EnricherQLParser(tokens);

        assertEquals(query.replaceAll("\\s+", ""), parser.query_output().getText());

    }

    @Test
    public void JoinWithByKeyShouldWork() {
        String query = "JOIN SELECT * FROM TABLE rb_input BY rb_key USING jClass";

        ANTLRInputStream inputStream = new ANTLRInputStream(query);
        EnricherQLLexer lexer = new EnricherQLLexer(inputStream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        EnricherQLParser parser = new EnricherQLParser(tokens);

        assertEquals(query.replaceAll("\\s+", ""), parser.query_join().getText());

    }

    @Test
    public void JoinWithoutByKeyShouldWork() {

        String query = "JOIN SELECT * FROM STREAM rb_input USING jClass";

        ANTLRInputStream inputStream = new ANTLRInputStream(query);
        EnricherQLLexer lexer = new EnricherQLLexer(inputStream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        EnricherQLParser parser = new EnricherQLParser(tokens);

        assertEquals(query.replaceAll("\\s+", ""), parser.query_join().getText());

    }

    @Test
    public void EnricherWithShouldWork() {
        String query = "ENRICH WITH pclass1";

        ANTLRInputStream inputStream = new ANTLRInputStream(query);
        EnricherQLLexer lexer = new EnricherQLLexer(inputStream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        EnricherQLParser parser = new EnricherQLParser(tokens);

        assertEquals(query.replaceAll("\\s+", ""), parser.query_enrich_with().getText());
    }

    @Test
    public void SimpleSelectShouldWork() {

        String query = "SELECT * FROM STREAM rb_input " +
                "JOIN SELECT a,b FROM STREAM rb_input2 USING jpackageClass " +
                "INSERT INTO STREAM rb_output";

        ANTLRInputStream inputStream = new ANTLRInputStream(query);
        EnricherQLLexer lexer = new EnricherQLLexer(inputStream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        EnricherQLParser parser = new EnricherQLParser(tokens);

        assertEquals(query.replaceAll("\\s+", ""), parser.query().getText());

    }

    @Test
    public void ComplexSelectShouldWork() {

        String query = "SELECT * FROM STREAM rb_input " +
                "JOIN SELECT a,b,c FROM STREAM rb_input2 USING jClass1 " +
                "JOIN SELECT * FROM TABLE rb_input3 USING jClass2 "+
                "JOIN SELECT x,y FROM STREAM rb_input4 USING jClass3 " +
                "INSERT INTO TABLE rb_output";

        ANTLRInputStream inputStream = new ANTLRInputStream(query);
        EnricherQLLexer lexer = new EnricherQLLexer(inputStream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        EnricherQLParser parser = new EnricherQLParser(tokens);

        assertEquals(query.replaceAll("\\s+", ""), parser.query().getText());

    }
}
