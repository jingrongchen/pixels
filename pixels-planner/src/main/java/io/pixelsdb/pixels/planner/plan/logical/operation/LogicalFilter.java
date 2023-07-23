package io.pixelsdb.pixels.planner.plan.logical.operation;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
/**
 * 
 * LogicalFilter operation
 * @author Jingrong
 * @date 2023-07-19
 */
public class LogicalFilter {

    private String filterOpName;

    private String filterOpKind;

    private List<operation> operands;

    public LogicalFilter (String filterOpName, String filterOpKind, List<operation> operands) {
        this.filterOpName = filterOpName;
        this.filterOpKind = filterOpKind;
        this.operands = operands;
    }

    public LogicalFilter (String filterOpName, String filterOpKind) {
        this.filterOpName = filterOpName;
        this.filterOpKind = filterOpKind;
        this.operands = new ArrayList<>();
    }

    public LogicalFilter() {
        this.operands = new ArrayList<>();
    }

    public class operation {

        int[] OpFieldId;

        String opName;

        String opKind;

        // when kind is equal, the literal and literalType are used are null
        Object literal;

        String literalType;
        
        public operation(int[] OpFieldId, String opName, String opKind, Object literal, String literalType) {
            this.OpFieldId = OpFieldId;
            this.opName = opName;
            this.opKind = opKind;
            this.literal = literal;
            this.literalType = literalType;
        }

        public int[] getInputFieldId() {
            return OpFieldId;
        }

        public void setInputFieldId(int[] OpFieldId) {
            this.OpFieldId = OpFieldId;
        }

        public String getOpName() {
            return opName;
        }

        public void setOpName(String opName) {
            this.opName = opName;
        }

        public String getOpKind() {
            return opKind;
        }

        public void setOpKind(String opKind) {
            this.opKind = opKind;
        }

        public Object getLiteral() {
            return literal;
        }

        public void setLiteral(String literal) {
            this.literal = literal;
        }

        public String getLiteralType() {
            return literalType;
        }

        public void setLiteralType(String literalType) {
            this.literalType = literalType;
        }

        @Override
        public String toString() {
            return "operation [OpFieldId=" + Arrays.toString(OpFieldId) + ", literal=" + literal + ", literalType=" + literalType
                    + ", opKind=" + opKind + ", opName=" + opName + "]";
        }

    }

    public void addOperation(int[] OpFieldId, String opName, String opKind, Object literal, String literalType) {
        this.operands.add(new operation(OpFieldId, opName, opKind, literal, literalType));
    }

    public String getFilterOpName() {
        return filterOpName;
    }

    public void setFilterOpName(String filterOpName) {
        this.filterOpName = filterOpName;
    }

    public String getFilterOpKind() {
        return filterOpKind;
    }

    public void setFilterOpKind(String filterOpKind) {
        this.filterOpKind = filterOpKind;
    }

    public List<operation> getOperands() {
        return operands;
    }

    public void setOperands(List<operation> operands) {
        this.operands = operands;
    }

    public void addOperand(operation operand) {
        this.operands.add(operand);
    }

    //TODO: add support for filter
    public String getFilterString(){
        return "to be implemented";
    }

    @Override
    public String toString() {
        return "LogicalFilter {filterOpKind=" + filterOpKind + ", filterOpName=" + filterOpName + ", operands="
                + operands.toString() + "}";
    }

}
