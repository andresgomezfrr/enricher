package rb.ks.model.antlr4;

import java.util.ArrayList;
import java.util.List;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class Query {

    Select select;
    List<Join> joins;
    Stream insertTopic;


    public Query(Select select, List<Join> joins, Stream insertTopic) {
        this.select = checkNotNull(select, "SELECT cannot be null");
        this.joins = checkNotNull(joins, "JOINS cannot be null");
        this.insertTopic = checkNotNull(insertTopic, "INSERT cannot be null");

    }

    public void setSelect(Select newSelect) {
        select = newSelect;
    }

    public Select getSelect() {
        return select;
    }

    public void setJoins(List<Join> newJoinsList) {
        joins = newJoinsList;
    }

    public List<Join> getJoins() {
        return joins;
    }

    public void addJoin(Join newJoin) {
        joins.add(newJoin);
    }

    public void setinsert(Stream newStream) {
        insertTopic = newStream;
    }

    public Stream getInsert() {
        return insertTopic;
    }

    public void validate() {
        checkNotNull(select, "SELECT cannot be null");
        select.validate();

        checkNotNull(joins, "JOINS cannot be null");
        joins.forEach(Join::validate);

        checkNotNull(insertTopic, "INSERT cannot be null");
        insertTopic.validate();
    }

}
