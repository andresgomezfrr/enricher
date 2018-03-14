package io.wizzie.ks.query;

import io.wizzie.ks.query.antlr4.Query;
import io.wizzie.ks.query.compiler.EnricherQLLexer;
import io.wizzie.ks.query.compiler.EnricherQLParser;
import io.wizzie.ks.query.compiler.EnricherQLVisitor;
import io.wizzie.ks.query.internal.EnricherErrorListener;
import io.wizzie.ks.query.internal.EnricherQLBaseVisitorImpl;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

public class EnricherCompiler {

    public static Query parse(String source) {

        ANTLRInputStream input = new ANTLRInputStream(source);
        EnricherQLLexer lexer = new EnricherQLLexer(input);
        lexer.removeErrorListeners();
        lexer.addErrorListener(EnricherErrorListener.INSTANCE);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        EnricherQLParser parser = new EnricherQLParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(EnricherErrorListener.INSTANCE);
        ParseTree tree = parser.query();

        EnricherQLVisitor<Query> eval = new EnricherQLBaseVisitorImpl();

        return eval.visit(tree);
    }
}
