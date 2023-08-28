package io.pixelsdb.pixels.planner.plan.logical.calcite;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.fastjson.annotation.JSONType;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.predicate.ColumnFilter;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;


@JSONType(includes = {"type", "nullable", "name" , "precision", "scale"})
public class field {
    
    @JSONField(name = "type", ordinal = 0)
    private final String type;

    @JSONField(name = "nullable", ordinal = 1)
    private final boolean nullable;

    @JSONField(name = "name", ordinal = 2)
    private final String name;

    @JSONField(name="precision", ordinal = 3)
    private final int precision;

    @JSONField(name="scale", ordinal = 4)
    private final int scale;


    @JSONCreator
    public field(String type, boolean nullable, String name)
    {
        this.type = type;
        this.nullable = nullable;
        this.name = name;
        this.precision = 0;
        this.scale = 0;
    }

    @JSONCreator
    public field(String type, boolean nullable, String name,int precision)
    {
        this.type = type;
        this.nullable = nullable;
        this.name = name;
        this.precision = precision;
        this.scale = 0;
    }


    @JSONCreator
    public field(String type, boolean nullable, String name, int precision, int scale)
    {
        this.type = type;
        this.nullable = nullable;
        this.name = name;
        this.precision = precision;
        this.scale = scale;
    }


    public field()
    {
        this.type = "";
        this.nullable = false;
        this.name = "";
        this.precision = 0;
        this.scale = 0;
    }

    public static field empty(String type, boolean nullable)
    {
        return new field(type, nullable, "");
    }

    public String getType()
    {
        return type;
    }

    public boolean getNullable()
    {
        return nullable;
    }

    public String getName()
    {
        return name;
    }

    public int getPrecision()
    {
        return precision;
    }

    public int getScale()
    {
        return scale;
    }


}
