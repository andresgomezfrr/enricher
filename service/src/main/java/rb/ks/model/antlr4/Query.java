package rb.ks.model.antlr4;

import java.util.ArrayList;
import java.util.List;

public class Query {

    Select select;
    List<Join> joinsList = new ArrayList<>();
    Stream insert;

    public void setSelect(Select newSelect) {
        select = newSelect;
    }

    public Select getSelect() {
        return select;
    }

    public void setJoins(List<Join> newJoinsList) {
        joinsList = newJoinsList;
    }

    public List<Join> getJoins() {
        return joinsList;
    }

    public void addJoin(Join newJoin) {
        joinsList.add(newJoin);
    }

    public void setinsert(Stream newStream) {
        insert = newStream;
    }

    public Stream getInsert() {
        return insert;
    }

    public void validate() {
        select.validate();

        joinsList.forEach(Join::validate);

        insert.validate();
    }

}
