package rb.ks.model.antlr4.output;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class Output {

    public enum Type {STREAM, TABLE}

    protected Type type = Type.STREAM;

    protected String id;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = checkNotNull(id, "ID is required");
    }

    public Output setType(Type type) {
        this.type = checkNotNull(type, "Type is required");
        return this;
    }

    public Type getType() {
        return type;
    }

    public void validate() {
        checkNotNull(type, "Type is required");
        checkNotNull(id, "ID is required");
    }
}
