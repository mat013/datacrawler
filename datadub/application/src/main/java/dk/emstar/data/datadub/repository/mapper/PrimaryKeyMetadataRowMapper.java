package dk.emstar.data.datadub.repository.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.springframework.jdbc.core.RowMapper;

import dk.emstar.data.datadub.metadata.ColumnMetaData;
import dk.emstar.data.datadub.metadata.PrimaryKeyMetadata;

public class PrimaryKeyMetadataRowMapper implements RowMapper<PrimaryKeyMetadata> {

	private final Map<String, ColumnMetaData> columnMap;

	
	public PrimaryKeyMetadataRowMapper(Map<String, ColumnMetaData> columnMap) {
		this.columnMap = columnMap;
	}

	@Override
	public PrimaryKeyMetadata mapRow(ResultSet rs, int rowNum) throws SQLException {
		ColumnMetaData column = columnMap.get(rs.getString("COLUMN_NAME"));
		PrimaryKeyMetadata result = new PrimaryKeyMetadata(rs.getString("PK_NAME"), column, rs.getLong("KEY_SEQ"));
		return result;
	}
}