package io.pixelsdb.pixels.planner.plan.logical.calcite;
import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.fastjson.annotation.JSONType;


@JSONType(includes = {"literal", "type"})
public class condition {

    @JSONField(name = "literal", ordinal = 0)
    private final String literal;

    @JSONField(name = "type", ordinal = 1)
    private final type type;

    @JSONCreator
    public condition(String literal, type type)
    {
        this.literal = literal;
        this.type = type;
    }
    
    public condition()
    {
        this.literal = null;
        this.type = null;
    }

    public String getLiteral() {
        return literal;
    }

    public type getType() {
        return type;
    }
    
}
