package rb.ks.model.antlr4;

import rb.ks.model.antlr4.output.Output;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class InsertInto extends Output {

    private boolean isInner = false;

    public static InsertInto insertInto() {
        return new InsertInto();
    }

    public boolean isInner() {
        return isInner;
    }

    public InsertInto isInner(boolean isInner) {
        this.isInner = isInner;
        return this;
    }

    public String id() {
        return id;
    }

    public InsertInto id(String id) {
        this.id = checkNotNull(id, "ID is required");
        return this;
    }

    public InsertInto type(Type type) {
        this.type = checkNotNull(type, "Type is required");
        return this;
    }

    public Type type() {
        return type;
    }

    public void validate() {
        checkNotNull(id, "ID is required");
        checkNotNull(type, "Type is required");
    }
}
