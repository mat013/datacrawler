package dk.emstar.data.datadub.metadata;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

import dk.emstar.data.datadub.events.OnCellChangeSink;
import dk.emstar.data.datadub.modification.RowAction;
import dk.emstar.data.datadub.modification.RowActionSelector;

/**
 * 
 * TODO add merge operation, mutable or immutable, add upstream and downstream
 * columns that are not possible to find from datadictionary
 * 
 * @author mat013
 *
 */
public class TableData implements Iterable<Map<String, Object>>, OnCellChangeSink {

    private static final Logger logger = LoggerFactory.getLogger(TableData.class);

    private final TableMetadata tableMetaData;
    private final Table<RowId, String, Object> data = HashBasedTable.create();

    private final List<OnCellChangeSink> onCellChangeSinks = Lists.newArrayList();

    public TableData(TableMetadata tableMetaData, List<Map<String, Object>> rows) {
        this.tableMetaData = tableMetaData;

        long count = 0;
        for (Map<String, Object> row : rows) {
            count++;
            for (Entry<String, Object> entry : row.entrySet()) {
                Object value = entry.getValue();
                try {
                    if (value != null) {
                        data.put(new RowId(count, this), entry.getKey(), value);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(String.format("Error when inserting %x, %s, %s: %s", count, entry.getKey(), value, e.getMessage()), e);
                }
            }
        }
    }

    public TableMetadata getMetadata() {
        return tableMetaData;
    }

    public Table<RowId, String, Object> getData() {
        return data;
    }

    public Set<TableNameIdentifier> getDownstreamTables() {
        return tableMetaData.getDownstreamColumns().values().stream().flatMap(o -> o.stream()).map(o -> o.getForeignTableNameIdenfier())
                .collect(Collectors.toSet());
    }

    public Set<TableNameIdentifier> getUpstreamTables() {
        return tableMetaData.getUpstreamColumns().values().stream().flatMap(o -> o.stream()).map(o -> o.getForeignTableNameIdenfier())
                .collect(Collectors.toSet());
    }

    public Map<Map<String, Object>, RowId> getPrimaryKeysIndex() {
        Map<Map<String, Object>, RowId> result = Maps.newHashMap();

        List<PrimaryKeyMetadata> primaryKeys = tableMetaData.getPrimaryKeys();
        for (Entry<RowId, Map<String, Object>> row : data.rowMap().entrySet()) {
            Map<String, Object> key = Maps.newHashMap();
            for (PrimaryKeyMetadata primaryKeyMetadata : primaryKeys) {
                ColumnMetaData column = primaryKeyMetadata.getColumn();
                String name = column.getName();
                key.put(name, row.getValue().get(name));
            }
            result.put(key, row.getKey());
        }

        return result;
    }

    public Table<RowId, String, Object> getPrimaryKeysValues() {
        Table<RowId, String, Object> result = HashBasedTable.create();

        List<PrimaryKeyMetadata> primaryKeys = tableMetaData.getPrimaryKeys();
        for (Entry<RowId, Map<String, Object>> row : data.rowMap().entrySet()) {
            for (PrimaryKeyMetadata primaryKeyMetadata : primaryKeys) {
                ColumnMetaData column = primaryKeyMetadata.getColumn();
                String name = column.getName();
                result.put(row.getKey(), name, row.getValue().get(name));
            }
        }

        return result;
    }

    public Object changeCell(String columnName, RowId row, Object to) {
        if (data.size() < row.getId()) {
            throw new IllegalArgumentException(
                    String.format("Index out of bounds Trying to access row %d in a table which has %d row(s)", row, data.size()));
        }

        if (!data.containsColumn(columnName)) {
            throw new IllegalArgumentException(
                    String.format("Column %s does not exist: %s are valid", columnName, Joiner.on(", ").join(data.columnKeySet())));
        }

        Object oldValue = data.put(row, columnName, to);
        logger.info("Changing column {} for row {} from {} to {}", columnName, row, oldValue, to);
        if (!oldValue.equals(to)) {
            ColumnMetaData columnMetaData = tableMetaData.getColumn(columnName);
            for (OnCellChangeSink sink : onCellChangeSinks) {
                sink.onCellChange(columnMetaData, oldValue, to);
            }
        }

        return oldValue;
    }

    public TableNameIdentifier getTableIdentifier() {
        return tableMetaData.getTableName();
    }

    @Override
    public String toString() {
        return String.format("%s", tableMetaData.getTableName());
    }

    // TODO another name with a formatter
    public String toContent() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Entry<RowId, Map<String, Object>> row : data.rowMap().entrySet()) {
            if (stringBuilder.length() != 0) {
                stringBuilder.append("\r\n");
            }

            for (String columnName : data.columnKeySet()) {
                stringBuilder.append(String.format("%s;", row.getValue().get(columnName)));
            }
        }

        return stringBuilder.toString();
    }

    public Map<String, Object> getRow(RowId rowId) {
        return data.rowMap().get(rowId);
    }

    // TODO should be moved out
    public Map<RowId, RowAction> determineRowActions(TableData mirrorTable, RowActionSelector rowAcceptor) {
        Map<Map<String, Object>, RowId> mirrorIndex = mirrorTable.getPrimaryKeysIndex();
        Map<RowId, RowAction> result = Maps.newHashMap();
        for (Entry<Map<String, Object>, RowId> indexRow : getPrimaryKeysIndex().entrySet()) {
            Map<String, Object> sourceRow = getRow(indexRow.getValue());
            Map<String, Object> mirrorRow = mirrorTable.getRow(mirrorIndex.get(indexRow.getKey()));
            result.put(indexRow.getValue(), rowAcceptor.selectAction(sourceRow, mirrorRow, tableMetaData, mirrorTable.getMetadata()));
        }
        return result;
    }

    // TODO is this the right way?
    public void remove(RowId row) {
        for (String column : data.row(row).keySet()) {
            data.remove(row, column);
        }
    }

    @Override
    public Iterator<Map<String, Object>> iterator() {
        return data.rowMap().values().iterator();
    }

    public void adviceCellChange(OnCellChangeSink onCellChange) {
        onCellChangeSinks.add(onCellChange);
    }

    @Override
    public void onCellChange(ColumnMetaData columnMetaData, Object matchingValue, Object value) {
        // check if the data is relevant by looking at the upstream column
        List<UpstreamReferenceColumnMetaData> columnsToBeChecked = tableMetaData.getUpstreamConstraints(columnMetaData);
        if (columnsToBeChecked.isEmpty()) {
            return;
        }

        for (Entry<RowId, Map<String, Object>> row : data.rowMap().entrySet()) {
            for (UpstreamReferenceColumnMetaData column : columnsToBeChecked) {
                Object oldValue = row.getValue().get(column.getColumnName());
                if (matchingValue.equals(oldValue)) {
                    changeCell(column.getColumnName(), row.getKey(), value);
                }
            }
        }
    }
}
