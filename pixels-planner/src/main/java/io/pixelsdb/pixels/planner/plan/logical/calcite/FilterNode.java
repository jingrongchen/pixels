package io.pixelsdb.pixels.planner.plan.logical.calcite;
import java.util.List;
import java.util.ArrayList;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
public class FilterNode {
    
    private operand[] operands;

    private condition[] conditions;

    private operation[] operations;

    private ObjectMapper mapper = new ObjectMapper();

    public FilterNode() {
    }


    public void processNode(JsonNode jnode) throws IOException{
        
        List<operand> operandList = new ArrayList<>();
        List<condition> conditionList = new ArrayList<>();
        List<operation> operationList = new ArrayList<>();


        for(JsonNode tmepNode : jnode){

            System.out.println("process node");

            if(tmepNode.get("literal")!=null){
                condition tmp = mapper.readerFor(condition.class).readValue(tmepNode);
                conditionList.add(tmp);
            }else if (tmepNode.get("input")!=null) {
                operand tmp = mapper.readerFor(operand.class).readValue(tmepNode);
                operandList.add(tmp);
            }else if(tmepNode.get("operands")!=null){
                operation tmp = mapper.readerFor(operation.class).readValue(tmepNode);
                operationList.add(tmp);
            }
        }

        this.operands = operandList.toArray(new operand[operandList.size()]);
        this.conditions = conditionList.toArray(new condition[conditionList.size()]);
        this.operations = operationList.toArray(new operation[operationList.size()]);
    }

    public operand[] getOperands() {
        return operands;
    }

    public void setOperands(operand[] operands) {
        this.operands = operands;
    }

    public condition[] getConditions() {
        return conditions;
    }

    public void setConditions(condition[] conditions) {
        this.conditions = conditions;
    }

    public operation[] getOperations() {
        return operations;
    }

    public void setOperations(operation[] operations) {
        this.operations = operations;
    }

    







}
