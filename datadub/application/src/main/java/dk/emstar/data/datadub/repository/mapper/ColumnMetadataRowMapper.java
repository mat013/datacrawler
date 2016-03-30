package dk.emstar.data.datadub.repository.mapper;

import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import dk.emstar.data.datadub.metadata.ColumnMetaData;
import dk.emstar.data.datadub.metadata.TableNameIdentifier;

@Component
public class ColumnMetadataRowMapper implements RowMapper<ColumnMetaData> {

    @Override
    public ColumnMetaData mapRow(ResultSet rs, int rowNum) throws SQLException {

        // TODO use flyweight pattern
        TableNameIdentifier tableNameIdentifier = new TableNameIdentifier(rs.getString("TABLE_CAT"), rs.getString("TABLE_SCHEM"),
                rs.getString("TABLE_NAME"));

        ColumnMetaData result = new ColumnMetaData(tableNameIdentifier, rs.getString("COLUMN_NAME"), JDBCType.valueOf(rs.getInt("DATA_TYPE")), false);
        return result;
    }

}