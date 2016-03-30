package dk.emstar.data.datadub.metadata;

import com.google.common.base.Joiner;

public class TableNameIdentifier {
	private final String catalog;
	private final String schema;
	private final String tableName;

	public TableNameIdentifier(String schema, String tableName) {
		this(null, schema, tableName);
	}

	public TableNameIdentifier(String catalog, String schema, String tableName) {
		this.catalog = catalog;
		this.schema = schema;
		this.tableName = tableName;
	}

	public String getFullQualifiedName() {
		return Joiner.on(".").skipNulls().join(catalog, schema, tableName);
	}
	
	public String getCatalog() {
		return catalog;
	}

	public String getSchema() {
		return schema;
	}

	public String getTableName() {
		return tableName;
	}

	@Override
	public String toString() {
		return String.format("(%s, %s, %s)", catalog, schema, tableName);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((catalog == null) ? 0 : catalog.hashCode());
		result = prime * result + ((schema == null) ? 0 : schema.hashCode());
		result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TableNameIdentifier other = (TableNameIdentifier) obj;
		if (catalog == null) {
			if (other.catalog != null)
				return false;
		} else if (!catalog.equals(other.catalog))
			return false;
		if (schema == null) {
			if (other.schema != null)
				return false;
		} else if (!schema.equals(other.schema))
			return false;
		if (tableName == null) {
			if (other.tableName != null)
				return false;
		} else if (!tableName.equals(other.tableName))
			return false;
		return true;
	}
}
