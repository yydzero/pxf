package org.greenplum.pxf.plugins.hdfs.parquet;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.filter.ColumnIndexOperandNode;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.OperandNode;
import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.OperatorNode;
import org.greenplum.pxf.api.filter.TreeVisitor;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.booleanColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.floatColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.apache.parquet.filter2.predicate.FilterApi.not;
import static org.apache.parquet.filter2.predicate.FilterApi.notEq;
import static org.apache.parquet.filter2.predicate.FilterApi.or;

/**
 * This is the implementation of {@link TreeVisitor} for Parquet.
 * <p>
 * The class visits all the {@link Node}s from the expression tree,
 * and builds a simple (single {@link FilterCompat.Filter} class) for
 * {@link org.greenplum.pxf.plugins.hdfs.ParquetFileAccessor} to use for its
 * scan.
 */
public class ParquetRecordFilterBuilder implements TreeVisitor {

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final Map<String, Type> fields;
    private final List<ColumnDescriptor> columnDescriptors;
    private final Deque<FilterPredicate> filterQueue;

    /**
     * Constructor
     *
     * @param columnDescriptors the list of column descriptors
     * @param originalFields    a map of field names to types
     */
    public ParquetRecordFilterBuilder(List<ColumnDescriptor> columnDescriptors, Map<String, Type> originalFields) {
        this.columnDescriptors = columnDescriptors;
        this.filterQueue = new LinkedList<>();
        this.fields = originalFields;
    }

    @Override
    public Node before(Node node, int level) {
        return node;
    }

    @Override
    public Node visit(Node node, int level) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();

            if (!operator.isLogical()) {
                processSimpleColumnOperator(operatorNode);
            }
        }
        return node;
    }

    @Override
    public Node after(Node node, int level) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();
            if (operator.isLogical()) {
                processLogicalOperator(operator);
            }
        }
        return node;
    }

    /**
     * Returns the built record filter
     *
     * @return the built record filter
     */
    public FilterCompat.Filter getRecordFilter() {
        FilterPredicate predicate = filterQueue.poll();
        if (!filterQueue.isEmpty()) {
            throw new IllegalStateException("Filter queue is not empty after visiting all nodes");
        }
        return predicate != null ? FilterCompat.get(predicate) : FilterCompat.NOOP;
    }

    private void processLogicalOperator(Operator operator) {
        FilterPredicate right = filterQueue.poll();
        FilterPredicate left = null;

        if (right == null) {
            throw new IllegalStateException("Unable to process logical operator " + operator.toString());
        }

        if (operator == Operator.AND || operator == Operator.OR) {
            left = filterQueue.poll();

            if (left == null) {
                throw new IllegalStateException("Unable to process logical operator " + operator.toString());
            }
        }

        switch (operator) {
            case AND:
                filterQueue.push(and(left, right));
                break;
            case OR:
                filterQueue.push(or(left, right));
                break;
            case NOT:
                filterQueue.push(not(right));
                break;
        }
    }

    /**
     * Handles simple column-operator-constant expressions.
     *
     * @param operatorNode the operator node
     */
    private void processSimpleColumnOperator(OperatorNode operatorNode) {

        Operator operator = operatorNode.getOperator();
        ColumnIndexOperandNode columnIndexOperand = operatorNode.getColumnIndexOperand();
        OperandNode valueOperand = null;

        if (operator != Operator.IS_NULL && operator != Operator.IS_NOT_NULL) {
            valueOperand = operatorNode.getValueOperand();
            if (valueOperand == null) {
                throw new IllegalArgumentException(
                        String.format("Operator %s does not contain an operand", operator));
            }
        }

        ColumnDescriptor columnDescriptor = columnDescriptors.get(columnIndexOperand.index());
        String filterColumnName = columnDescriptor.columnName();
        Type type = fields.get(filterColumnName);

        // INT96 and FIXED_LEN_BYTE_ARRAY cannot be pushed down
        // for more details look at org.apache.parquet.filter2.dictionarylevel.DictionaryFilter#expandDictionary
        // where INT96 and FIXED_LEN_BYTE_ARRAY are not dictionary values

        FilterPredicate simpleFilter;
        switch (operator) {
            case IS_NULL:
            case EQUALS:
            case NOOP:
                // NOT boolean wraps a NOOP
                //       NOT
                //        |
                //       NOOP
                //        |
                //    ---------
                //   |         |
                //   4        true
                // that needs to be replaced with equals
                simpleFilter = getEquals(type.getName(),
                        type.asPrimitiveType().getPrimitiveTypeName(),
                        type.getOriginalType(),
                        valueOperand);
                break;
            case LESS_THAN:
                simpleFilter = getLessThan(type.getName(),
                        type.asPrimitiveType().getPrimitiveTypeName(),
                        type.getOriginalType(),
                        valueOperand);
                break;
            case GREATER_THAN:
                simpleFilter = getGreaterThan(type.getName(),
                        type.asPrimitiveType().getPrimitiveTypeName(),
                        type.getOriginalType(),
                        valueOperand);
                break;
            case LESS_THAN_OR_EQUAL:
                simpleFilter = getLessThanOrEqual(type.getName(),
                        type.asPrimitiveType().getPrimitiveTypeName(),
                        type.getOriginalType(),
                        valueOperand);
                break;
            case GREATER_THAN_OR_EQUAL:
                simpleFilter = getGreaterThanOrEqual(type.getName(),
                        type.asPrimitiveType().getPrimitiveTypeName(),
                        type.getOriginalType(),
                        valueOperand);
                break;
            case IS_NOT_NULL:
            case NOT_EQUALS:
                simpleFilter = getNotEquals(type.getName(),
                        type.asPrimitiveType().getPrimitiveTypeName(),
                        type.getOriginalType(),
                        valueOperand);
                break;
            default:
                throw new UnsupportedOperationException("not supported " + operator);
        }

        filterQueue.push(simpleFilter);
    }

    private FilterPredicate getLessThan(String columnName,
                                        PrimitiveType.PrimitiveTypeName parquetType,
                                        OriginalType originalType,
                                        OperandNode operand) {
        String value = operand.toString();

        switch (parquetType) {
            case INT32:
                return lt(intColumn(columnName), getIntegerForINT32(originalType, value));

            case INT64:
                return lt(longColumn(columnName), Long.parseLong(value));

            case BINARY:
                return lt(binaryColumn(columnName), Binary.fromString(value));

            case FLOAT:
                return lt(floatColumn(columnName), Float.parseFloat(value));

            case DOUBLE:
                return lt(doubleColumn(columnName), Double.parseDouble(value));

            default:
                throw new UnsupportedOperationException(String.format("Column %s of type %s is not supported",
                        columnName, operand.getDataType()));
        }
    }

    private FilterPredicate getLessThanOrEqual(String columnName,
                                               PrimitiveType.PrimitiveTypeName parquetType,
                                               OriginalType originalType,
                                               OperandNode operand) {
        String value = operand.toString();

        switch (parquetType) {
            case INT32:
                return ltEq(intColumn(columnName), getIntegerForINT32(originalType, value));

            case INT64:
                return ltEq(longColumn(columnName), Long.parseLong(value));

            case BINARY:
                return ltEq(binaryColumn(columnName), Binary.fromString(value));

            case FLOAT:
                return ltEq(floatColumn(columnName), Float.parseFloat(value));

            case DOUBLE:
                return ltEq(doubleColumn(columnName), Double.parseDouble(value));

            default:
                throw new UnsupportedOperationException(String.format("Column %s of type %s is not supported",
                        columnName, operand.getDataType()));
        }
    }

    private FilterPredicate getGreaterThan(String columnName,
                                           PrimitiveType.PrimitiveTypeName parquetType,
                                           OriginalType originalType,
                                           OperandNode operand) {
        String value = operand.toString();

        switch (parquetType) {
            case INT32:
                return gt(intColumn(columnName), getIntegerForINT32(originalType, value));

            case INT64:
                return gt(longColumn(columnName), Long.parseLong(value));

            case BINARY:
                return gt(binaryColumn(columnName), Binary.fromString(value));

            case FLOAT:
                return gt(floatColumn(columnName), Float.parseFloat(value));

            case DOUBLE:
                return gt(doubleColumn(columnName), Double.parseDouble(value));

            default:
                throw new UnsupportedOperationException(String.format("Column %s of type %s is not supported",
                        columnName, parquetType));
        }
    }

    private FilterPredicate getGreaterThanOrEqual(String columnName,
                                                  PrimitiveType.PrimitiveTypeName parquetType,
                                                  OriginalType originalType,
                                                  OperandNode operand) {
        String value = operand.toString();

        switch (parquetType) {
            case INT32:
                return gtEq(intColumn(columnName), getIntegerForINT32(originalType, value));

            case INT64:
                return gtEq(longColumn(columnName), Long.parseLong(value));

            case BINARY:
                return gtEq(binaryColumn(columnName), Binary.fromString(value));

            case FLOAT:
                return gtEq(floatColumn(columnName), Float.parseFloat(value));

            case DOUBLE:
                return gtEq(doubleColumn(columnName), Double.parseDouble(value));

            default:
                throw new UnsupportedOperationException(String.format("Column %s of type %s is not supported",
                        columnName, operand.getDataType()));
        }
    }

    private FilterPredicate getEquals(String columnName,
                                      PrimitiveType.PrimitiveTypeName parquetType,
                                      OriginalType originalType,
                                      OperandNode operand) {
        String value = operand == null ? null : operand.toString();
        switch (parquetType) {
            case INT32:
                return eq(intColumn(columnName), getIntegerForINT32(originalType, value));

            case INT64:
                return eq(longColumn(columnName), value == null ? null : Long.parseLong(value));

            case BOOLEAN:
                return eq(booleanColumn(columnName), value == null ? null : Boolean.parseBoolean(value));

            case BINARY:
                return eq(binaryColumn(columnName), value == null ? null : Binary.fromString(value));

            case FLOAT:
                return eq(floatColumn(columnName), value == null ? null : Float.parseFloat(value));

            case DOUBLE:
                return eq(doubleColumn(columnName), value == null ? null : Double.parseDouble(value));

            default:
                throw new UnsupportedOperationException(String.format("Column %s of type %s is not supported",
                        columnName, parquetType));
        }
    }

    private FilterPredicate getNotEquals(String columnName,
                                         PrimitiveType.PrimitiveTypeName parquetType,
                                         OriginalType originalType,
                                         OperandNode operand) {
        String value = operand == null ? null : operand.toString();
        switch (parquetType) {
            case INT32:
                return notEq(intColumn(columnName), getIntegerForINT32(originalType, value));

            case INT64:
                return notEq(longColumn(columnName), value == null ? null : Long.parseLong(value));

            case BOOLEAN:
                return notEq(booleanColumn(columnName), value == null ? null : Boolean.parseBoolean(value));

            case BINARY:
                return notEq(binaryColumn(columnName), value == null ? null : Binary.fromString(value));

            case FLOAT:
                return notEq(floatColumn(columnName), value == null ? null : Float.parseFloat(value));

            case DOUBLE:
                return notEq(doubleColumn(columnName), value == null ? null : Double.parseDouble(value));

            default:
                throw new UnsupportedOperationException(String.format("Column %s of type %s is not supported",
                        columnName, parquetType));
        }
    }

    private Integer getIntegerForINT32(OriginalType originalType, String value) {
        if (value == null) return null;
        if (originalType == OriginalType.DATE) {
            // Number of days since epoch
            LocalDate localDateValue = LocalDate.parse(value);
            LocalDate epoch = LocalDate.ofEpochDay(0);
            return (int) ChronoUnit.DAYS.between(epoch, localDateValue);
        }
        return Integer.parseInt(value);
    }
}
