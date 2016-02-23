package dk.emstar.data.datadub.metadata;

import java.util.Map.Entry;

import com.google.common.collect.Lists;
import com.google.common.collect.Table;

public class ValuePropagator implements OnCellChangeSink {

	private TableData dependentTable;
	private ColumnMetaData columnToChange;

	public ValuePropagator(TableData dependentTable, ColumnMetaData columnToChange) {
		this.dependentTable = dependentTable;
		this.columnToChange = columnToChange;
	}

	@Override
	public void onCellChange(ColumnMetaData columnMetaData, Object matchingValue, Object value) {
		
		Table<Long, String, Object> tableData = dependentTable.getTableData();
		String columnName = columnToChange.getName();
		for(Entry<Long, Object> row : Lists.newArrayList(tableData.column(columnName).entrySet())) {
			Object currentValue = row.getValue();
			if(matchingValue.equals(currentValue)) {
				tableData.put(row.getKey(), columnName, value);
			}
		}
	}
}