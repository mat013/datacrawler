package dk.emstar.data.datadub.metadata;

import java.sql.Types;

public class PrimaryKeyMetadata {

	private final String constraintName;
	private final ColumnMetaData column;
	private final long columnIndex;

	public PrimaryKeyMetadata(String constraintName, ColumnMetaData column, long columnIndex) {
		this.constraintName = constraintName;
		this.column = column;
		this.columnIndex = columnIndex;
	}

	public String getMaxIdSql() {
		switch(column.getJdbcType()) {
		case NUMERIC:
		case DECIMAL:
			return getMaxIdNumericSql();
		default:
			throw new IllegalArgumentException(String.format("%d is not supported", column.getJdbcType()));
		}
		
	}
	
	private String getMaxIdNumericSql() {
		return String.format("select max(%s) from %s", column.getName(), column.getTableNameIdentifier().getFullQualifiedName());
	}

	public String getConstraintName() {
		return constraintName;
	}

	public ColumnMetaData getColumn() {
		return column;
	}

	public long getColumnIndex() {
		return columnIndex;
	}

	@Override
	public String toString() {
		return String.format("(%s, %s)", column, columnIndex);
	}
}
