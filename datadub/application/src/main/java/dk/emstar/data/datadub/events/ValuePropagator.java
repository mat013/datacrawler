package dk.emstar.data.datadub.events;

import java.util.Map.Entry;

import com.google.common.collect.Lists;
import com.google.common.collect.Table;

import dk.emstar.data.datadub.metadata.ColumnMetaData;
import dk.emstar.data.datadub.metadata.RowId;
import dk.emstar.data.datadub.metadata.TableData;

public class ValuePropagator implements OnCellChangeSink {

	private TableData dependentTable;
	private ColumnMetaData columnToChange;

	public ValuePropagator(TableData dependentTable, ColumnMetaData columnToChange) {
		this.dependentTable = dependentTable;
		this.columnToChange = columnToChange;
	}

	@Override
	public void onCellChange(ColumnMetaData columnMetaData, Object matchingValue, Object value) {
		
		Table<RowId, String, Object> data = dependentTable.getData();
		String columnName = columnToChange.getName();
		for(Entry<RowId, Object> row : Lists.newArrayList(data.column(columnName).entrySet())) {
			Object currentValue = row.getValue();
			if(matchingValue.equals(currentValue)) {
				data.put(row.getKey(), columnName, value);
			}
		}
	}
}