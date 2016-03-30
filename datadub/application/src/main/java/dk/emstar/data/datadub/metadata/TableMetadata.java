package dk.emstar.data.datadub.metadata;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class TableMetadata {
    private final TableNameIdentifier tableNameIdentifier;

    private final List<PrimaryKeyMetadata> primaryKeys;

    private final List<ColumnMetaData> columns;
    private final Map<String, List<DownstreamReferenceColumnMetaData>> downstreamConstraints;
    private final Map<String, List<UpstreamReferenceColumnMetaData>> upstreamConstraints;

    public final String ROWACTION_COLUMN = "*ROWACTION*";

    public TableMetadata(TableNameIdentifier tableNameIdentifier, List<ColumnMetaData> columns, List<PrimaryKeyMetadata> primaryKeys,
            Map<String, List<UpstreamReferenceColumnMetaData>> upstreamConstraints,
            Map<String, List<DownstreamReferenceColumnMetaData>> downStreamConstraints) {
        this.tableNameIdentifier = tableNameIdentifier;
        this.primaryKeys = primaryKeys;
        this.upstreamConstraints = ImmutableMap.copyOf(upstreamConstraints);
        this.columns = ImmutableList.copyOf(columns);
        this.downstreamConstraints = ImmutableMap.copyOf(downStreamConstraints);
    }

    public TableNameIdentifier getTableName() {
        return tableNameIdentifier;
    }

    public List<PrimaryKeyMetadata> getPrimaryKeys() {
        return primaryKeys;
    }

    public List<ColumnMetaData> getColumns() {
        return columns;
    }

    public ColumnMetaData getColumn(String columnName) {
        return columns.stream().filter(o -> columnName.equals(o.getName())).findFirst().get();
    }

    public Map<String, List<DownstreamReferenceColumnMetaData>> getDownstreamColumns() {
        return downstreamConstraints;
    }

    public Map<String, List<UpstreamReferenceColumnMetaData>> getUpstreamColumns() {
        return upstreamConstraints;
    }

    public List<List<DownstreamReferenceColumnMetaData>> getDownstreamConstraints(TableNameIdentifier tableNameIdentifier) {
        List<List<DownstreamReferenceColumnMetaData>> result = Lists.newArrayList();
        for (List<DownstreamReferenceColumnMetaData> downstreamReferenceColumnMetaData : downstreamConstraints.values()) {
            if (!downstreamReferenceColumnMetaData.isEmpty()) {
                if (tableNameIdentifier.equals(downstreamReferenceColumnMetaData.get(0).getForeignTableNameIdenfier())) {
                    result.add(downstreamReferenceColumnMetaData);
                }
            }
        }
        return result;
    }

    public List<List<UpstreamReferenceColumnMetaData>> getUpstreamConstraints(TableNameIdentifier tableNameIdentifier) {
        List<List<UpstreamReferenceColumnMetaData>> result = Lists.newArrayList();
        for (List<UpstreamReferenceColumnMetaData> upstreamReferenceColumnMetaData : upstreamConstraints.values()) {
            if (!upstreamReferenceColumnMetaData.isEmpty()) {
                if (tableNameIdentifier.equals(upstreamReferenceColumnMetaData.get(0).getForeignTableNameIdenfier())) {
                    result.add(upstreamReferenceColumnMetaData);
                }
            }
        }
        return result;
    }

    public List<UpstreamReferenceColumnMetaData> getUpstreamConstraints(ColumnMetaData columnMetaData) {
        List<UpstreamReferenceColumnMetaData> result = Lists.newArrayList();
        TableNameIdentifier columnTableIdentifier = columnMetaData.getTableNameIdentifier();
        String columnName = columnMetaData.getName();
        for (List<UpstreamReferenceColumnMetaData> upstreamConstraint : upstreamConstraints.values()) {
            for (UpstreamReferenceColumnMetaData upstreamReferenceColumn : upstreamConstraint) {
                if (columnTableIdentifier.equals(upstreamReferenceColumn.getForeignTableNameIdenfier())
                        && columnName.equals(upstreamReferenceColumn.getForeignColumnName())) {
                    result.add(upstreamReferenceColumn);
                }
            }
        }

        return result;
    }

    @Override
    public String toString() {
        return String.format("(%s, %s, %s)", tableNameIdentifier, columns, downstreamConstraints);
    }
}
