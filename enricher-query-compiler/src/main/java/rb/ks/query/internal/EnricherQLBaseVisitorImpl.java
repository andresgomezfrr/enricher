package rb.ks.query.internal;

import org.antlr.v4.runtime.RuleContext;
import rb.ks.model.PlanModel;
import rb.ks.model.antlr4.Join;
import rb.ks.model.antlr4.Query;
import rb.ks.model.antlr4.Select;
import rb.ks.model.antlr4.Stream;
import rb.ks.query.compiler.EnricherQLBaseVisitor;
import rb.ks.query.compiler.EnricherQLParser;

import java.util.List;
import java.util.stream.Collectors;

public class EnricherQLBaseVisitorImpl extends EnricherQLBaseVisitor {

    @Override
    public PlanModel visitQuery(EnricherQLParser.QueryContext ctx) {

        List<String> selectDimensions = ctx.dimensions().id().stream().map(id -> id.getText()).collect(Collectors.toList());

        List<Stream> selectInputStreams = ctx.streams().id().stream().map(id -> new Stream(id.getText(), ctx.type().toString().matches("TABLE"))).collect(Collectors.toList());

        List<Join> joins = ctx.query_join().stream().map(join -> {
            boolean table = join.type().toString().equals("TABLE");
            List<String> joinDimensions = join.dimensions().id().stream().map(id -> id.toString()).collect(Collectors.toList());
            String className = join.className().toString();
            String joinStream = join.id().toString();

            return new Join(new Stream(joinStream, table), className, joinDimensions);
        }).collect(Collectors.toList());

        Stream output = new Stream(ctx.query_output().id().getText(), ctx.query_output().type().getText().matches("TABLE"));

        Query query = new Query(
                new Select(selectDimensions, selectInputStreams),
                joins,
                output);

        return null;
    }

}
