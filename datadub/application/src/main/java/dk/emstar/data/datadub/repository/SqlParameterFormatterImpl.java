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
			case Types.FLOAT:
			case Types.DOUBLE:
			case Types.DECIMAL:
			case Types.INTEGER:
			case Types.REAL:
			case Types.SMALLINT:
			case Types.NUMERIC:
				return value.toString();
			case Types.CHAR:
			case Types.VARCHAR:
				return String.format("'%s'", value.toString());
			case Types.TIME:
			case Types.TIME_WITH_TIMEZONE:
			case Types.TIMESTAMP:
			case Types.TIMESTAMP_WITH_TIMEZONE:
			case Types.DATE:
			default:
				throw new IllegalArgumentException(
						String.format("Type % is not yet supported", columnData.getJdbcType()));
		}
	}
}