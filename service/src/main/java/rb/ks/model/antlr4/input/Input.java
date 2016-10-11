package rb.ks.model.antlr4.input;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class Input {

    public enum Type {STREAM, TABLE}

    protected Type type = Type.STREAM;

    protected String id;

    public static Input input() {
        return new Input();
    }

    public Input id(String id) {
        this.id = checkNotNull(id, "ID is required");
        return this;
    }

    public String id() {
        return id;
    }

    public Input type(Type type) {
        this.type = checkNotNull(type, "Type is required");
        return this;
    }

    public Type type() {
        return type;
    }

    public void validate() {
        checkNotNull(type, "Type is required");
        checkNotNull(id, "ID is required");
    }

}
