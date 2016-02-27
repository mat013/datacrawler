package dk.emstar.data.datadub.metadata;

public class RowId {

	private final long id;
	private final TableNameIdentifier tableNameIdentifier;

	public RowId(long id, TableNameIdentifier tableNameIdentifier) {
		this.id = id;
		this.tableNameIdentifier = tableNameIdentifier;
	}

	public long getId() {
		return id;
	}

	public TableNameIdentifier getTableNameIdentifier() {
		return tableNameIdentifier;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (id ^ (id >>> 32));
		result = prime * result + ((tableNameIdentifier == null) ? 0 : tableNameIdentifier.hashCode());
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
		RowId other = (RowId) obj;
		if (id != other.id)
			return false;
		if (tableNameIdentifier == null) {
			if (other.tableNameIdentifier != null)
				return false;
		} else if (!tableNameIdentifier.equals(other.tableNameIdentifier))
			return false;
		return true;
	}
}
