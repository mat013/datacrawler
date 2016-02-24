package dk.emstar.data.datadub.repository.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import dk.emstar.data.datadub.metadata.TableNameIdentifier;
import dk.emstar.data.datadub.metadata.UpstreamReferenceColumnMetaData;

@Component
public class UpstreamReferenceColumnMetadataRowMapper implements RowMapper<UpstreamReferenceColumnMetaData> {

	@Override
	public UpstreamReferenceColumnMetaData mapRow(ResultSet rs, int rowNum) throws SQLException {
		TableNameIdentifier tableNameIdenfier = new TableNameIdentifier(rs.getString("PKTABLE_CAT"), rs.getString("PKTABLE_SCHEM"), rs.getString("PKTABLE_NAME"));
		UpstreamReferenceColumnMetaData result = new UpstreamReferenceColumnMetaData(rs.getString("PK_NAME"), 
				rs.getString("FKCOLUMN_NAME"), 
				tableNameIdenfier,
				rs.getString("PKCOLUMN_NAME"),
				rs.getLong("KEY_SEQ"));
		return result;
	}
	
}