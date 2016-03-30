package dk.emstar.data.datadub.repository;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

import dk.emstar.data.datadub.metadata.ColumnMetaData;
import dk.emstar.data.datadub.metadata.DownstreamReferenceColumnMetaData;
import dk.emstar.data.datadub.metadata.PrimaryKeyMetadata;
import dk.emstar.data.datadub.metadata.RowId;
import dk.emstar.data.datadub.metadata.TableData;
import dk.emstar.data.datadub.metadata.TableMetadata;
import dk.emstar.data.datadub.metadata.TableNameIdentifier;
import dk.emstar.data.datadub.metadata.UpstreamReferenceColumnMetaData;
import dk.emstar.data.datadub.modification.RowAction;

// TODO we should only copy real columns and not metadata columns

public class TableDataRepositoryImpl implements TableDataRepository {

	private static final Logger logger = LoggerFactory.getLogger(TableDataRepositoryImpl.class);
	
	private final NamedParameterJdbcOperations jdbcOperations;
	private final MetadataRepository metadataRepository;

	private final String schema;

	private final Map<ColumnMetaData, NextPrimaryKeyGenerator> nextPrimaryKeyGenerators = Maps.newHashMap();
	private final Map<Integer, NextPrimaryKeyGenerator> defaultNextPrimaryKeyGenerators = Maps.newHashMap();
	
	@Autowired
	public TableDataRepositoryImpl(String schema, NamedParameterJdbcOperations sourceJdbcTemplate,
			MetadataRepository metadataRepository) {  
		this.schema = schema;
		this.jdbcOperations = sourceJdbcTemplate;
		this.metadataRepository = metadataRepository;
	}

	@Override
	public String getSchema() {
		return schema;
	}

	@Override
	public void apply(Map<RowId, RowAction> rowActionResult, TableData mirrorTable, TableData sourceTable) {
		List<PrimaryKeyMetadata> primaryKeyMetadatas = mirrorTable.getMetadata().getPrimaryKeys();
		if(primaryKeyMetadatas.size() != 1) {
			String columnNames = Joiner.on(", ").join(primaryKeyMetadatas.stream()
					.map(o -> o.getColumn().getName())
					.collect(Collectors.toList()));
			throw new IllegalStateException(String.format("Currently no composite keys are supported. This has %s columns", columnNames));
		}

		PrimaryKeyMetadata primaryKey = primaryKeyMetadatas.get(0);
		long id = getNextId(primaryKey.getMaxIdSql());
		for (Entry<RowId, RowAction> rowActionRow : rowActionResult.entrySet()) {
			switch(rowActionRow.getValue()) {
				case USE_EXISTING_ROW_LOOKUP_KEY:
					id = getNextId(id);
					sourceTable.changeCell(primaryKey.getColumn().getName(), rowActionRow.getKey(), id);
					break;
				case IGNORE:
					sourceTable.remove(rowActionRow.getKey());
					break;
				default:
				
			}
		}
	}

	private long getNextId(long id) {
		return id + 1;
	}

	@Override
	public TableData findMirrorTable(TableData table) {
		Table<RowId, String, Object> keysForOtherPartTable = table.getPrimaryKeysValues();
		TableData result = get(new TableNameIdentifier(schema, table.getTableIdentifier().getTableName()), keysForOtherPartTable);
		return result;
	}

	@Override
	public TableData get(TableNameIdentifier tableNameIdentifier, Table<RowId, String, Object> keysForOtherPartTable) {
		Map<String, String> columnToFieldMap = keysForOtherPartTable.columnKeySet().stream()
				.collect(Collectors.toMap(o -> o, o -> o));
		
		return get(tableNameIdentifier, columnToFieldMap, keysForOtherPartTable);
	}

	// TODO use a function object strategy
	private Long getNextId(String sql) {
		logger.info(sql);
		
		Long result = jdbcOperations.queryForObject(sql, Maps.newHashMap(), Long.class);
		return result == null ? 0L : result;
	}
	
	@Override
	public void persist(TableData sourceTable, Map<RowId, RowAction> rowActionResult, TableData destination) {
		TableMetadata metadata = destination.getMetadata();
		List<ColumnMetaData> columns = metadata.getColumns();
		String sql = String.format("insert into %s(%s) values(%s)", 
				metadata.getTableName().getFullQualifiedName(), 
				Joiner.on(", ").join(columns.stream().map(o -> o.getName()).collect(Collectors.toList())),
				Joiner.on(", ").join(columns.stream().map(o -> String.format(":%s", o.getName())).collect(Collectors.toList())));
		logger.info(sql);
		
		for (Entry<RowId, RowAction> rowActionRow : rowActionResult.entrySet()) {
			Map<String, Object> rowData = sourceTable.getRow(rowActionRow.getKey());
			Map<String, Object> args = Maps.newHashMap();
			for (ColumnMetaData column : columns) {
				args.put(column.getName(), rowData.get(column.getName()));
			}

			logger.info("Persisting: {}", args);
			jdbcOperations.update(sql, args);
		}
	}

	@Override
	public List<String> getInsertStatements(TableData tableData, Map<RowId, RowAction> rowActionResult, TableData destination) {
		return getInsertStatements(tableData, rowActionResult, destination, new SqlParameterFormatterImpl());
	}

	private List<String> getInsertStatements(TableData tableData, Map<RowId, RowAction> rowActionResult,
			TableData destination, SqlParameterFormatter formatter) {
		TableMetadata metadata = destination.getMetadata();
		List<ColumnMetaData> columns = metadata.getColumns();
		
		String sql = String.format("insert into %s(%s) values(%s)", 
				metadata.getTableName().getFullQualifiedName(), 
				Joiner.on(", ").join(columns.stream().map(o -> o.getName()).collect(Collectors.toList())),
				"%s");

		List<String> result = Lists.newArrayList();
		for (Entry<RowId, RowAction> rowActionRow : rowActionResult.entrySet()) {
			Map<String, Object> rowData = tableData.getRow(rowActionRow.getKey());
			List<String> args = Lists.newArrayList();
			for (ColumnMetaData column : columns) {
				args.add(formatter.format(column, rowData.get(column.getName())));
			}
			result.add(String.format(sql, Joiner.on(", ").join(args)));
			
		}
		
		return result;
	}
	
	
	@Override
	public TableData get(TableNameIdentifier tableIdentifier, Map<String, String> columnToFieldMap, Table<RowId, String, Object> keys) {
		// TODO check for duplicates
		
		TableMetadata tableMetaData = metadataRepository.getMetaData(tableIdentifier);

		// TODO move creation of sql to tableMetadata.getRowSelectionSql();
		StringBuilder sqlBuilder = getSelectAllBuilder(tableIdentifier);

		// TODO ensure we support many rows 2000+ 
		// TODO ensure we support composite keys
		int count = 1;
		Map<String, Object> map = Maps.newHashMap();
		for (Map<String, Object> row : keys.rowMap().values()) {
			sqlBuilder.append("\r\nor (1 = 1");
			for (Entry<String, Object> entry : row.entrySet()) {
				sqlBuilder.append(" and ");
				sqlBuilder.append(entry.getKey());
				sqlBuilder.append(" = ");
				sqlBuilder.append(String.format(":%d", count));
				sqlBuilder.append(" ");
				map.put(String.format("%d", count), entry.getValue());
			}
			
			sqlBuilder.append(")");
			count++;
		}

		String sql = sqlBuilder.toString();
		logger.info(sql);
		List<Map<String, Object>> data = jdbcOperations.queryForList(sql, map);
		TableData result = new TableData(tableMetaData, data);
		
		return result;
	}

	@Override
	public TableData getByPrimaryKey(TableNameIdentifier tableIdentifier, TableData tableData) {
		TableMetadata metaData = metadataRepository.getMetaData(tableIdentifier);

		List<List<DownstreamReferenceColumnMetaData>> downstreamColumnConstriants = metaData.getDownstreamConstraints(tableData.getTableIdentifier());
		
		String sql = buildSelectOnDownstream(tableIdentifier, downstreamColumnConstriants);
		logger.info("Retrieving by primary {}", sql);

		List<Map<String, Object>> result = readDataDistinceOnPrimaryKey(tableData, sql, metaData);
		return new TableData(metaData, result);
	}

	@Override
	public TableData getByReferenceKey(TableNameIdentifier tableIdentifier, TableData tableData) {
		TableMetadata metaData = metadataRepository.getMetaData(tableIdentifier);

		List<List<UpstreamReferenceColumnMetaData>> upstreamColumnConstriants = metaData.getUpstreamConstraints(tableData.getTableIdentifier());

		String sql = buildSelectOnUpstream(tableIdentifier, upstreamColumnConstriants);
		logger.info("Retrieving by reference {}", sql);

		List<Map<String, Object>> result = readDataDistinceOnPrimaryKey(tableData, sql, metaData);
		return new TableData(metaData, result);
	}
	
	private String buildSelectOnDownstream(TableNameIdentifier tableIdentifier,
			List<List<DownstreamReferenceColumnMetaData>> downstreamColumnConstriants) {
		StringBuilder sqlBuilder = getSelectAllBuilder(tableIdentifier);
		sqlBuilder.append("\r\n");
		
		for (List<DownstreamReferenceColumnMetaData> downstreamColumnConstriant : downstreamColumnConstriants) {
			sqlBuilder.append(" or (1 = 0");
			for (DownstreamReferenceColumnMetaData downstreamColumn : downstreamColumnConstriant) {
				sqlBuilder.append(" or (");
				sqlBuilder.append(String.format("%s is null or %s = :%s", downstreamColumn.getColumnName(), downstreamColumn.getColumnName(), downstreamColumn.getForeignColumnName()));
				sqlBuilder.append(")");
			}
			sqlBuilder.append(")");
		}
		
		return sqlBuilder.toString();
	}

	private String buildSelectOnUpstream(TableNameIdentifier tableIdentifier,
			List<List<UpstreamReferenceColumnMetaData>> upstreamColumnConstriants) {
		StringBuilder sqlBuilder = getSelectAllBuilder(tableIdentifier);
		sqlBuilder.append("\r\n");
		
		for (List<UpstreamReferenceColumnMetaData> upstreamColumnConstriant : upstreamColumnConstriants) {
			sqlBuilder.append(" or (1 = 0");
			for (UpstreamReferenceColumnMetaData upstreamColumn : upstreamColumnConstriant) {
				sqlBuilder.append(" or (");
				sqlBuilder.append(String.format("%s is null or %s = :%s", upstreamColumn.getColumnName(), upstreamColumn.getColumnName(), upstreamColumn.getForeignColumnName()));
				sqlBuilder.append(")");
			}
			sqlBuilder.append(")");
		}
		
		return sqlBuilder.toString();
	}

	private List<Map<String, Object>> readDataDistinceOnPrimaryKey(TableData tableData, String sql, TableMetadata metaData) {
		Map<List<Object>, Map<String, Object>> result = Maps.newHashMap();
		
		for (Map<String, Object> row : tableData) {
			Map<String, Object> arguments = Maps.newHashMap(row);
			for (ColumnMetaData column : tableData.getMetadata().getColumns()) {
				String columnName = column.getName();
				if(!arguments.containsKey(columnName)) {
					arguments.put(columnName, null);
				}
			}
			
			logger.info("Arguments: {}", arguments);
			
			List<Map<String, Object>> subResult = jdbcOperations.queryForList(sql, arguments);
			for (final Map<String, Object> subResultRow : subResult) {
				List<Object> primaryKey =  metaData.getPrimaryKeys().stream()
					.map(o -> subResultRow.get(o.getColumn().getName()))
					.collect(Collectors.toList());
				
				logger.info("Found item with primary key: {}", primaryKey);
				if(result.containsKey(primaryKey)) {
					logger.info("Item already exist. It will be overwritten");
				}
				result.put(primaryKey, subResultRow);
			}
			
		}
		return Lists.newArrayList(result.values());
	}

	private StringBuilder getSelectAllBuilder(TableNameIdentifier tableIdentifier) {
		return new StringBuilder(String.format("select * from %s where 1 = 0", tableIdentifier.getFullQualifiedName()));
	}

	@Override
	public TableData getByMirror(TableNameIdentifier tableIdentifier, TableData tableData) {
		TableMetadata metaData = metadataRepository.getMetaData(tableIdentifier);
		String sql = buildSelectOnPrimaryKey(tableIdentifier, metaData.getPrimaryKeys());
		logger.info("Retrieving by primary keys from mirror table {}", sql);

		List<Map<String, Object>> result = readDataDistinceOnPrimaryKey(tableData, sql, metaData);
		return new TableData(metaData, result);
	}

	private String buildSelectOnPrimaryKey(TableNameIdentifier tableIdentifier, List<PrimaryKeyMetadata> primaryKeys) {
		StringBuilder sqlBuilder = getSelectAllBuilder(tableIdentifier);
		sqlBuilder.append("\r\n");
		
			sqlBuilder.append(" or (1 = 0");
			for (PrimaryKeyMetadata primaryKey : primaryKeys) {
				sqlBuilder.append(" or (");
				sqlBuilder.append(String.format("%s is null or %s = :%s", primaryKey.getColumn().getName(),
						primaryKey.getColumn().getName(), 
						primaryKey.getColumn().getName()));
				sqlBuilder.append(")");
			}
			sqlBuilder.append(")");
		
		return sqlBuilder.toString();
	}
	
	@Override
	public String toString() {
		return schema;
	}
}
