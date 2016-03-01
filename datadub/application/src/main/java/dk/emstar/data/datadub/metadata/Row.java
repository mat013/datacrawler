package dk.emstar.data.datadub.metadata;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import dk.emstar.data.datadub.modification.RowAction;

public class Row implements Iterable<ColumnValue>{

	private TableData owner;
	private RowId id;
	private Map<String, Object> values;
	
	public RowId getId() {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public Iterator<ColumnValue> iterator() {
		throw new RuntimeException("Not Implemented");
	}

	/**
	 * TODO also support for columnMetaData
	 * 
	 * @param columnName
	 * @param value
	 */
	public void modify(String columnName, Object value) {
		throw new RuntimeException("Not implemented");
	}

	public ColumnValue getValueFor(String columnName) {
		throw new RuntimeException("Not implemented");
	}
	
	public Object getOriginalValueFor(String columnName) {
		throw new RuntimeException("Not implemented");
	}
	
	public Object reset(String columnName) {
		throw new RuntimeException("Not implemented");
	}
	
	public List<ColumnValue> allColumns() {
		throw new RuntimeException("Not implemented");
	}

	public boolean isModified() {
		throw new RuntimeException("Not implemented");
	}
	
	public RowAction getRowAction() {
		throw new RuntimeException("Not implemented");
	}
	
	public void setRowAction() {
		throw new RuntimeException("Not implemented");
	}
	
	// %old.value
	// %rowaction
	// %dirty
}
