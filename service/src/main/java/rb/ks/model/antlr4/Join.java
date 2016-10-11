package rb.ks.model.antlr4;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class Join {

    Select select;

    Class rClass;

    public static Join join() {
        return new Join();
    }

    public Join using(Class myClass) {
        checkNotNull(select, "Select is required");
        this.rClass = checkNotNull(myClass, "Class is required");
        return this;
    }

    public Class using() {
        return rClass;
    }

    public Join query(Select select) {
        this.select = checkNotNull(select, "Select is required");
        return this;
    }

    public Select query() {
        return select;
    }

    public void validate() {
        checkNotNull(select, "Select is required");
        checkNotNull(rClass, "Class is required");
        select.validate();
    }

}
