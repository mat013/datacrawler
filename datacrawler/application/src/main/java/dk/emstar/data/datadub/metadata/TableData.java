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

import dk.emstar.data.datadub.fun.RowAction;
import dk.emstar.data.datadub.fun.RowActionSelector;

public class TableData implements Iterable<Map<String, Object>>, OnCellChangeSink {

//	private static final String ROWACTION = "*ROWACTION*";

	private static final Logger logger = LoggerFactory.getLogger(TableData.class);
	
	private final TableMetadata tableMetaData;
	private final Table<Long, String, Object> tableData = HashBasedTable.create();

	@Deprecated
	private final Map<TableNameIdentifier, TableData> downstreamTables = Maps.newHashMap();
	
	@Deprecated
	private final Map<TableNameIdentifier, TableData> upstreamTables = Maps.newHashMap();
	
	@Deprecated
	private final Map<ColumnMetaData, List<OnCellChangeSink>> sinkMap = Maps.newHashMap();
	private final List<OnCellChangeSink> onCellChangeSinks = Lists.newArrayList();
	
	public TableData(TableMetadata tableMetaData, List<Map<String, Object>> rows) {
		this.tableMetaData = tableMetaData;
		
		long count = 0;
		for (Map<String, Object> row : rows) {
			count++;
			for (Entry<String, Object> entry : row.entrySet()) {
				Object value = entry.getValue();
				try {
					if(value != null) {
						tableData.put(count, entry.getKey(), value);
					}
				} catch(Exception e) {
					throw new RuntimeException(String.format("Error when inserting %x, %s, %s: %s", count, entry.getKey(), value, e.getMessage()), e);
				}
			}
			
//			tableData.put(count, ROWACTION, "ROWACTION");
		}
	}

	public TableMetadata getMetadata() {
		return tableMetaData;
	}

	public Table<Long, String, Object> getTableData() {
		return tableData;
	}

	public Set<TableNameIdentifier> getDownstreamTables() {
		return tableMetaData.getDownstreamColumns().values().stream()
				.flatMap(o -> o.stream())
				.map(o -> o.getForeignTableNameIdenfier())
				.collect(Collectors.toSet());
	}

	public Set<TableNameIdentifier> getUpstreamTables() {
		return tableMetaData.getUpstreamColumns().values().stream()
				.flatMap(o -> o.stream())
				.map(o -> o.getForeignTableNameIdenfier())
				.collect(Collectors.toSet());
	}

	public Map<Map<String, Object>, Long> getPrimaryKeysIndex() {
		Map<Map<String, Object>, Long> result = Maps.newHashMap();

		List<PrimaryKeyMetadata> primaryKeys = tableMetaData.getPrimaryKeys();
		for (Entry<Long, Map<String, Object>> row : tableData.rowMap().entrySet()) {
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
	
	public Table<Long, String, Object> getPrimaryKeysValues() {
		Table<Long, String, Object> result = HashBasedTable.create();

		List<PrimaryKeyMetadata> primaryKeys = tableMetaData.getPrimaryKeys();
		for (Entry<Long, Map<String, Object>> row : tableData.rowMap().entrySet()) {
			for (PrimaryKeyMetadata primaryKeyMetadata : primaryKeys) {
				ColumnMetaData column = primaryKeyMetadata.getColumn();
				String name = column.getName();
				result.put(row.getKey(), name, row.getValue().get(name));
			}
		}

		return result;
	}
	

	@Deprecated
	public Table<Long, String, Object> getKeysValuesForUpstream(TableNameIdentifier tableNameIdentifier) {
		Table<Long, String, Object> result = HashBasedTable.create();
//
//		List<UpstreamReferenceColumnMetadata> upstreamColumns = tableMetadata.getUpstreamColumns(tableNameIdentifier);
//		Set<Map<String, Object>> unique = Sets.newHashSet();
//
//		for (Entry<Long, Map<String, Object>> row : tableData.rowMap().entrySet()) {
//			Map<String, Object> keys = Maps.newHashMap();
//			Map<String, Object> rowData = row.getValue();
//			for (UpstreamReferenceColumnMetadata upStreamColumn : upstreamColumns) {
//				keys.put(upStreamColumn.getForeignColumnName(), rowData.get(upStreamColumn.getColumnName()));
//			}
//			unique.add(keys);
//		}
//		
//		long count = 0;
//		for (Map<String, Object> map : unique) {
//			for (Entry<String, Object> entry : map.entrySet()) {
//				result.put(count++, entry.getKey(), entry.getValue());
//			}
//		}
//
//		return result;
		throw new RuntimeException("Not implemented");
	}
	

	@Deprecated
	public Table<Long, String, Object> getKeysValuesForDownstream(TableNameIdentifier tableNameIdentifier) {
//		Table<Long, String, Object> result = HashBasedTable.create();
//
//		List<DownstreamReferenceColumnMetaData> downstreamColumnOnTable = tableMetadata.getDownstreamColumns(tableNameIdentifier);
//		
//		List<PrimaryKeyMetadata> primaryKeys = tableMetadata.getPrimaryKeys();
//		long count = 0;
//		for (Entry<Long, Map<String, Object>> row : tableData.rowMap().entrySet()) {
//			for (PrimaryKeyMetadata primaryKeyMetadata : primaryKeys) {
//				ColumnMetadata column = primaryKeyMetadata.getColumn();
//				try {
//				String name = column.getName();
//					DownstreamReferenceColumnMetaData referenceMetadata = downstreamColumnOnTable.stream().filter(o -> name.equals(o.getColumnName())).findFirst().get();
//					result.put(count++, referenceMetadata.getForeignColumnName(), row.getValue().get(name));
//				} catch(Exception e) {
//					throw new RuntimeException(String.format("Exception caught while handling column %s: %s", column, e.getMessage()), e);
//				}
//			}
//			count++;
//		}
//
//		return result;
		throw new RuntimeException("Not implemented");
	}

	
	@Deprecated
	public void addDownstream(TableData downstreamTable) {
		downstreamTables.put(downstreamTable.getTableIdentifier(), downstreamTable);
		// connect both ways
		//		downstreamTable.upstreamTables.put(getTableName(), this);
		
//		List<List<DownstreamReferenceColumnMetaData>> downstreamReferenceColumnMetadatas = tableMetadata.getDownstreamColumns(downstreamTable.getTableIdentifier());
//		for (DownstreamReferenceColumnMetaData downstreamReferenceColumnMetadata : downstreamReferenceColumnMetadatas) {
//			ColumnMetadata column = tableMetadata.getColumn(downstreamReferenceColumnMetadata.getColumnName());
//			
//			ColumnMetadata columnToChange = downstreamTable.get(downstreamReferenceColumnMetadata);
//			
//			List<OnCellChangeSink> sinks = sinkMap.get(column);
//			if(sinks == null) {
//				sinks = Lists.newArrayList();
//				sinkMap.put(column, sinks);
//			}
//			
//			sinks.add(new ValuePropagator(downstreamTable, columnToChange));
//		}
		throw new RuntimeException("Not implemented");
	}

	@Deprecated
	public void addUpstream(TableData upstreamTable) {
		upstreamTable.addDownstream(this);
	}

	@Deprecated
	private ColumnMetaData get(DownstreamReferenceColumnMetaData downstreamReferenceColumnMetadata) {
		String columnName = downstreamReferenceColumnMetadata.getForeignColumnName();
		return tableMetaData.getColumn(columnName);
	}

	public Object changeCell(String columnName, long row, Object to) {
		if(tableData.size() < row) {
			throw new IllegalArgumentException(String.format("Index out of bounds Trying to access row %d in a table which has %d row(s)", row, tableData.size()));
		}
		
		if(!tableData.containsColumn(columnName)) {
			throw new IllegalArgumentException(String.format("Column %s does not exist: %s are valid", columnName, Joiner.on(", ").join(tableData.columnKeySet())));
		}
		
		Object oldValue = tableData.put(row, columnName, to);
		logger.info("Changing column {} for row {} from {} to {}", columnName, row, oldValue, to);
		if(!oldValue.equals(to)) {
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
		for (Entry<Long, Map<String, Object>> row : tableData.rowMap().entrySet()) {
			if(stringBuilder.length() != 0) {
				stringBuilder.append("\r\n");
			}
			
			for (String columnName : tableData.columnKeySet()) {
				stringBuilder.append(String.format("%s;", row.getValue().get(columnName)));
			}
		}
		
		return stringBuilder.toString();
	}

	public Map<String, Object> getRow(Long rowId) {
		return tableData.row(rowId);
	}
	
	// TODO should be moved out
	public Map<Long, RowAction> determineRowActions(TableData mirrorTable, RowActionSelector rowAcceptor) {
		Map<Map<String, Object>, Long> mirrorIndex = mirrorTable.getPrimaryKeysIndex();
		Map<Long, RowAction> result = Maps.newHashMap();
		for (Entry<Map<String, Object>, Long> indexRow : getPrimaryKeysIndex().entrySet()) {
			Map<String, Object> sourceRow = getRow(indexRow.getValue());
			Map<String, Object> mirrorRow = mirrorTable.getRow(mirrorIndex.get(indexRow.getKey()));
			result.put(indexRow.getValue(), rowAcceptor.select(sourceRow, mirrorRow, tableMetaData, mirrorTable.getMetadata()));
		}
		return result;
	}

	// TODO is this the right way?
	public void remove(long row) {
		for (String column : tableData.row(row).keySet()) {
			tableData.remove(row, column);
		}
	}

	@Override
	public Iterator<Map<String, Object>> iterator() {
		return tableData.rowMap().values().iterator();
	}

	public void adviceCellChange(OnCellChangeSink onCellChange) {
		onCellChangeSinks.add(onCellChange);
	}

	@Override
	public void onCellChange(ColumnMetaData columnMetaData, Object matchingValue, Object value) {
		// check if the data is relevant by looking at the upstream column
		List<UpstreamReferenceColumnMetaData> columnsToBeChecked = tableMetaData.getUpstreamConstraints(columnMetaData);
		if(columnsToBeChecked.isEmpty()) {
			return;
		}
		
		for (Entry<Long, Map<String, Object>> row : tableData.rowMap().entrySet()) {
			for (UpstreamReferenceColumnMetaData column : columnsToBeChecked) {
				Object oldValue = row.getValue().get(column.getColumnName());
				if(matchingValue.equals(oldValue)) {
					changeCell(column.getColumnName(), row.getKey(), value);
				}
			}
		}
	}
}
