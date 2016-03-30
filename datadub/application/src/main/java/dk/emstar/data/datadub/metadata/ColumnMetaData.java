package dk.emstar.data.datadub.metadata;

import java.sql.JDBCType;

public class ColumnMetaData {
    private final TableNameIdentifier tableNameIdentifier;
    private final String name;
    private final JDBCType jdbcType;
    private final boolean isMetadataInformation;
    // private final boolean isNotNullColumn;

    public ColumnMetaData(TableNameIdentifier tableNameIdentifier, String name, JDBCType jdbcType, boolean isMetadataInformation) {
        this.tableNameIdentifier = tableNameIdentifier;
        this.name = name;
        this.jdbcType = jdbcType;
        this.isMetadataInformation = isMetadataInformation;
    }

    public TableNameIdentifier getTableNameIdentifier() {
        return tableNameIdentifier;
    }

    public String getName() {
        return name;
    }

    public JDBCType getJdbcType() {
        return jdbcType;
    }

    public boolean isMetadataInformation() {
        return isMetadataInformation;
    }

    @Override
    public String toString() {
        return String.format("(%s, %s, %s)", tableNameIdentifier, name, jdbcType);
    }
}
