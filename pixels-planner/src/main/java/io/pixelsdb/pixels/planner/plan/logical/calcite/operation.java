package io.pixelsdb.pixels.planner.plan.logical.calcite;
import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.fastjson.annotation.JSONType;

@JSONType(includes = {"op", "operands"})
public class operation {

    @JSONField(name = "op", ordinal = 0)
    private final op op;

    @JSONField(name = "operands", ordinal = 1)
    private final operand[] operands;

    @JSONCreator
    public operation(op op, operand[] operands)
    {
        this.op = op;
        this.operands = operands;
    }

    public operation()
    {
        this.op = null;
        this.operands = null;
    }

    public op getOp() {
        return op;
    }

    public operand[] getOperands() {
        return operands;
    }

    
}
