package rb.ks.model.antlr4;

import rb.ks.model.antlr4.input.Input;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class Select {

    Input input;

    List<Join> joins = new ArrayList<>();

    List<String> dimensions = new ArrayList<>();

    Optional<InsertInto> insertInto = Optional.empty();

    public static Select select() {
        return new Select();
    }

    public Select from(Input input) {
        this.input = checkNotNull(input, "Input is required");
        return this;
    }

    public Select join(Join join) {
        joins.add(checkNotNull(join, "Join cannot be null"));
        return this;
    }

    public List<Join> join() {
        return joins;
    }

    public Input from() {
        return input;
    }

    public Select dimensions(List<String> dimensions) {
        this.dimensions = checkNotNull(dimensions, "Dimension is required");
        return this;
    }

    public List<String> dimensions() {
        return dimensions;
    }

    public Select insertInto(InsertInto insertInto) {
        this.insertInto = Optional.ofNullable(insertInto);
        return this;
    }

    public InsertInto insertInto() {
        return insertInto.get();
    }

    public void validate() {
        checkNotNull(dimensions, "Dimension is required");
        checkNotNull(input, "Input is required");
        input.validate();

        if(insertInto.isPresent())
            insertInto.get().validate();

        if(!joins.isEmpty())
            joins.stream().forEach(join -> join.validate());
    }

}
