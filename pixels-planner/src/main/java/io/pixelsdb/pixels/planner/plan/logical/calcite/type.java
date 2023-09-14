package io.pixelsdb.pixels.planner.plan.logical.calcite;
import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.fastjson.annotation.JSONType;

@JSONType(includes = {"type", "nullable", "precision", "scale"})
public class type {
    
    @JSONField(name = "type", ordinal = 0)
    private final String type;

    @JSONField(name = "nullable", ordinal = 1)
    private final boolean nullable;

    @JSONField(name = "precision", ordinal = 2)
    private final int precision;

    @JSONField(name = "scale", ordinal = 3)
    private final int scale;

    @JSONCreator
    public type(String type, boolean nullable, int precision)
    {
        this.type = type;
        this.nullable = nullable;
        this.precision = precision;
        this.scale = 0;
    }

    @JSONCreator
    public type(String type, boolean nullable, int precision, int scale)
    {
        this.type = type;
        this.nullable = nullable;
        this.precision = precision;
        this.scale = scale;
    }

    public type()
    {
        this.type = null;
        this.nullable = false;
        this.precision = 0;
        this.scale = 0;
    }

    public String getType() {
        return type;
    }

    public boolean isNullable() {
        return nullable;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }
    

}
