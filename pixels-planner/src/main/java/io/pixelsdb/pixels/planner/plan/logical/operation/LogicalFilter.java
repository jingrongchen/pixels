package io.pixelsdb.pixels.planner.plan.logical.operation;

import java.util.List;
/**
 * 
 * LogicalFilter operation
 * @author Jingrong
 * @date 2023-07-19
 */
public class LogicalFilter {

    private String filterOpName;

    private String filterOpKind;

    private List<operand> operands;

    public class operand {

        int[] inputFieldId;

        String opName;

        String opKind;

        // when kind is equal, the literal and literalType are used are null
        String literal;

        String literalType;
        
        public operand(int[] inputFieldId, String opName, String opKind, String literal, String literalType) {
            this.inputFieldId = inputFieldId;
            this.opName = opName;
            this.opKind = opKind;
            this.literal = literal;
            this.literalType = literalType;
        }

        public int[] getInputFieldId() {
            return inputFieldId;
        }

        public void setInputFieldId(int[] inputFieldId) {
            this.inputFieldId = inputFieldId;
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

        public String getLiteral() {
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

    public List<operand> getOperands() {
        return operands;
    }

    public void setOperands(List<operand> operands) {
        this.operands = operands;
    }

    public void addOperand(operand operand) {
        this.operands.add(operand);
    }


}
