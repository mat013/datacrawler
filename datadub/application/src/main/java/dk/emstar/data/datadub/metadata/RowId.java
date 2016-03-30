package dk.emstar.data.datadub.metadata;

public class RowId {

	private final long id;
	private final TableData table;

	public RowId(long id) {
		this(id, null);
	}
	
	public RowId(long id, TableData table) {
		this.id = id;
		this.table = table;
	}

	public long getId() {
		return id;
	}

	public TableData getTable() {
		return table;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (id ^ (id >>> 32));
		result = prime * result + ((table == null) ? 0 : table.hashCode());
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
		if (table == null) {
			if (other.table != null)
				return false;
		} else if (!table.equals(other.table))
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return String.format("%d", id);
	}
}
