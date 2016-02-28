package dk.emstar.data.datadub.metadata;

public class UpstreamReferenceNonDeclaredColumnMetaData {
	
	private final String foreignConstraintName;
	private final String columnName;
	
	private final TableNameIdentifier foreignTableNameIdenfier;
	private final String foreignColumnName;
	private final long columnIndex;

	public UpstreamReferenceNonDeclaredColumnMetaData(String foreignConstraintName, String columnName, TableNameIdentifier foreignTableNameIdenfier, String foreignColumnName, long columnIndex) {
		this.foreignConstraintName = foreignConstraintName;
		this.columnName = columnName;
		this.foreignTableNameIdenfier = foreignTableNameIdenfier;
		this.foreignColumnName = foreignColumnName;
		this.columnIndex = columnIndex;
	}

	public String getForeignConstraintName() {
		return foreignConstraintName;
	}

	public String getColumnName() {
		return columnName;
	}

	
	public TableNameIdentifier getForeignTableNameIdenfier() {
		return foreignTableNameIdenfier;
	}

	public long getColumnIndex() {
		return columnIndex;
	}

	public String getForeignColumnName() {
		return foreignColumnName;
	}

	@Override
	public String toString() {
		return String.format("(%s, %s, %s, %s, %x)",
				foreignConstraintName, columnName, foreignTableNameIdenfier, foreignColumnName, columnIndex);
	}
}
