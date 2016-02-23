package dk.emstar.data.datadub.metadata;

public class ColumnMetaData {
	private final TableNameIdentifier tableNameIdentifier;
	private final String name;
	private final int jdbcType;
	
	public ColumnMetaData(TableNameIdentifier tableNameIdentifier, String name, int jdbcType) {
		this.tableNameIdentifier = tableNameIdentifier;
		this.name = name;
		this.jdbcType = jdbcType;
	}
	
	public TableNameIdentifier getTableNameIdentifier() {
		return tableNameIdentifier;
	}

	public String getName() {
		return name;
	}

	public int getJdbcType() {
		return jdbcType;
	}

	@Override
	public String toString() {
		return String.format("(%s, %s, %s)", tableNameIdentifier, name, jdbcType);
	}
}
