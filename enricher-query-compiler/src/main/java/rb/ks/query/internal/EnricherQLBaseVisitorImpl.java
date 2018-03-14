package rb.ks.query.internal;

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
    public Query visitQuery(EnricherQLParser.QueryContext ctx) {

        List<String> selectDimensions = ctx.dimensions().id().stream().map(id -> id.getText()).collect(Collectors.toList());

        List<Stream> selectInputStreams = ctx.streams().id().stream().map(id -> new Stream(id.getText(), ctx.type().toString().matches("TABLE"))).collect(Collectors.toList());

        List<Join> joins = ctx.query_join().stream().map(joinContext -> {
            boolean table = joinContext.type().getText().equals("TABLE");
            List<String> joinDimensions = joinContext.dimensions().id().stream().map(id -> id.getText()).collect(Collectors.toList());
            String className = joinContext.className().getText();
            String joinStream = joinContext.id().getText();

            return new Join(new Stream(joinStream, table), className, joinDimensions);
        }).collect(Collectors.toList());

        Stream output = new Stream(ctx.query_output().id().getText(), ctx.query_output().type().getText().matches("TABLE"));

        return new Query(new Select(selectDimensions, selectInputStreams), joins, output);
    }

}
