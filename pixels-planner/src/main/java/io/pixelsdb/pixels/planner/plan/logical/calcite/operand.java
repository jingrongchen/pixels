package io.pixelsdb.pixels.planner.plan.logical.calcite;
import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.fastjson.annotation.JSONType;


@JSONType(includes = {"input", "name", "type"})
public class operand {
    
    @JSONField(name = "input", ordinal = 0)
    private final String input;

    @JSONField(name = "name", ordinal = 1)
    private final String name;

    @JSONField(name = "type", ordinal = 2)
    private final type type;

    @JSONCreator
    public operand(String input, String name, type type)
    {
        this.input = input;
        this.name = name;
        this.type = type;
    }

    @JSONCreator
    public operand(String input, String name)
    {
        this.input = input;
        this.name = name;
        this.type = null;
    }

    public operand()
    {
        this.input = null;
        this.name = null;
        this.type = null;
    }


    public String getInput() {
        return input;
    }

    public String getName() {
        return name;
    }

    public type getType() {
        return type;
    }


}
