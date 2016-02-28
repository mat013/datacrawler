package dk.emstar.data.datadub.repository;

import java.sql.Types;

import dk.emstar.data.datadub.metadata.ColumnMetaData;

public class SqlParameterFormatterImpl implements SqlParameterFormatter {

	@Override
	public String format(ColumnMetaData columnData, Object value) {
		if(value == null) {
			return "null";
		}
		
		switch (columnData.getJdbcType()) {
			case FLOAT:
			case DOUBLE:
			case DECIMAL:
			case INTEGER:
			case REAL:
			case SMALLINT:
			case NUMERIC:
				return value.toString();
			case CHAR:
			case VARCHAR:
				return String.format("'%s'", value.toString());
			case TIME:
			case TIME_WITH_TIMEZONE:
			case TIMESTAMP:
			case TIMESTAMP_WITH_TIMEZONE:
			case DATE:
			default:
				throw new IllegalArgumentException(
						String.format("Type % is not yet supported", columnData.getJdbcType()));
		}
	}
}