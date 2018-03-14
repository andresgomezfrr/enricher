package zz.ks.query.internal;

import zz.ks.query.antlr4.Join;
import zz.ks.query.antlr4.Query;
import zz.ks.query.antlr4.Select;
import zz.ks.query.antlr4.Stream;
import zz.ks.query.compiler.EnricherQLBaseVisitor;
import zz.ks.query.compiler.EnricherQLParser;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static rb.ks.query.utils.Constants.__KEY;

public class EnricherQLBaseVisitorImpl extends EnricherQLBaseVisitor {

    @Override
    public Query visitQuery(EnricherQLParser.QueryContext ctx) {

        List<String> selectDimensions = ctx.dimensions().id().stream().map(id -> id.getText()).collect(Collectors.toList());

        if(selectDimensions.isEmpty())
            selectDimensions = Collections.singletonList("*");

        List<Stream> selectInputStreams = ctx.streams().id().stream().map(id -> new Stream(id.getText(), ctx.type().getText().equals("TABLE"))).collect(Collectors.toList());

        List<Join> joins = ctx.query_join().stream().map(joinContext -> {

            boolean table = joinContext.type().getText().equals("TABLE");

            List<String> joinDimensions = joinContext.dimensions().id().stream().map(id -> id.getText()).collect(Collectors.toList());

            if(joinDimensions.isEmpty())
                joinDimensions = Collections.singletonList("*");

            String className = joinContext.className().getText();
            String joinStream = joinContext.id().getText();

            EnricherQLParser.PartitionKeyContext partitionKeyContext;

            String partitionKey = ( partitionKeyContext = joinContext.partitionKey()) != null ? partitionKeyContext.getText() : __KEY;

            return new Join(new Stream(joinStream, table), className, joinDimensions, partitionKey);
        }).collect(Collectors.toList());

        List<String> enrichWiths = ctx.query_enrich_with().stream().map(enrichContext -> new String(enrichContext.className().getText())).collect(Collectors.toList());

        Stream output = new Stream(ctx.query_output().id().getText(), ctx.query_output().type().getText().matches("TABLE"));

        return new Query(new Select(selectDimensions, selectInputStreams), output, joins, enrichWiths);
    }

}
