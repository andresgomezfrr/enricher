package rb.ks.query;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import rb.ks.model.PlanModel;
import rb.ks.query.compiler.EnricherQLLexer;
import rb.ks.query.compiler.EnricherQLParser;
import rb.ks.query.compiler.EnricherQLVisitor;
import rb.ks.query.internal.EnricherErrorListener;
import rb.ks.query.internal.EnricherQLBaseVisitorImpl;

public class EnricherCompiler {

    public static PlanModel parse(String source) {

        ANTLRInputStream input = new ANTLRInputStream(source);
        EnricherQLLexer lexer = new EnricherQLLexer(input);
        lexer.removeErrorListeners();
        lexer.addErrorListener(EnricherErrorListener.INSTANCE);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        EnricherQLParser parser = new EnricherQLParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(EnricherErrorListener.INSTANCE);
        ParseTree tree = parser.query();

        EnricherQLVisitor<PlanModel> eval = new EnricherQLBaseVisitorImpl();

        return eval.visit(tree);
    }
}
