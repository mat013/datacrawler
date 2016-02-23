package dk.emstar.data.datadub.repository;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import dk.emstar.data.datadub.metadata.DownstreamReferenceColumnMetaData;
import dk.emstar.data.datadub.metadata.TableNameIdentifier;

@Component
public class DownstreamReferenceColumnMetadataRowMapper implements RowMapper<DownstreamReferenceColumnMetaData> {

	@Override
	public DownstreamReferenceColumnMetaData mapRow(ResultSet rs, int rowNum) throws SQLException {
		TableNameIdentifier tableNameIdenfier = new TableNameIdentifier(rs.getString("FKTABLE_CAT"), rs.getString("FKTABLE_SCHEM"), rs.getString("FKTABLE_NAME"));
		DownstreamReferenceColumnMetaData result = new DownstreamReferenceColumnMetaData(rs.getString("FK_NAME"), 
				rs.getString("PKCOLUMN_NAME"), 
				tableNameIdenfier,
				rs.getString("FKCOLUMN_NAME"),
				rs.getLong("KEY_SEQ"));
		return result;
	}
	
}