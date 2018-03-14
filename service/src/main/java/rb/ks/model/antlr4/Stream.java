package rb.ks.model.antlr4;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class Stream {

    String name;
    boolean isTable;

    public static Stream input() {
        return new Stream();
    }

    public void setTable(boolean value) {
        isTable = value;
    }

    public boolean isTable() {
        return isTable;
    }

    public Stream setName(String name) {
        this.name = checkNotNull(name, "ID is required");
        return this;
    }

    public String getName() {
        return name;
    }


    public void validate() {
        checkNotNull(name, "ID is required");
    }

}
