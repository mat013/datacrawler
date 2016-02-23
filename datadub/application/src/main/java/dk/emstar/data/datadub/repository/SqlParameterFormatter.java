package dk.emstar.data.datadub.repository;

import dk.emstar.data.datadub.metadata.ColumnMetaData;

public interface SqlParameterFormatter {
	public String format(ColumnMetaData columnData, Object value);
}