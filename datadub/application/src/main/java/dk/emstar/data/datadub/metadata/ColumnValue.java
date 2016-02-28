package dk.emstar.data.datadub.metadata;

public class ColumnValue {

	private final Row owner;
	private final ColumnMetaData columnMetadata;
	private final Object value;
	
	public ColumnValue(Row owner, ColumnMetaData columnMetadata, Object value) {
		this.owner = owner;
		this.columnMetadata = columnMetadata;
		this.value = value;
	}

	public Object modify(Object value) {
		throw new RuntimeException("Not implemented");
	}

	public Object value(Object value) {
		throw new RuntimeException("Not implemented");
	}
}
