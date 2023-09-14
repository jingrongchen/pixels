package io.pixelsdb.pixels.planner.plan.logical.calcite;
import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.fastjson.annotation.JSONType;


@JSONType(includes = {"name", "kind", "syntax"})
public class op {
    
    @JSONField(name = "name", ordinal = 0)
    private final String name;

    @JSONField(name = "kind", ordinal = 1)
    private final String kind;

    @JSONField(name = "syntax", ordinal = 2)
    private final String syntax;

    @JSONCreator
    public op(String name, String kind, String syntax)
    {
        this.name = name;
        this.kind = kind;
        this.syntax = syntax;
    }

    public op()
    {
        this.name = null;
        this.kind = null;
        this.syntax = null;
    }

    public String getName() {
        return name;
    }

    public String getKind() {
        return kind;
    }

    public String getSyntax() {
        return syntax;
    }


}
