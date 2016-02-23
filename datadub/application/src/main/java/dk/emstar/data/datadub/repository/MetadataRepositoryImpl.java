package dk.emstar.data.datadub.repository;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import dk.emstar.data.datadub.metadata.ColumnMetaData;
import dk.emstar.data.datadub.metadata.DownstreamReferenceColumnMetaData;
import dk.emstar.data.datadub.metadata.PrimaryKeyMetadata;
import dk.emstar.data.datadub.metadata.TableMetadata;
import dk.emstar.data.datadub.metadata.TableNameIdentifier;
import dk.emstar.data.datadub.metadata.UpstreamReferenceColumnMetaData;

public class MetadataRepositoryImpl implements MetadataRepository {

	private final DataSource dataSource;
	private final ColumnMetadataRowMapper columnMetadataRowMapper;
	private final DownstreamReferenceColumnMetadataRowMapper downstreamReferenceColumnMetadataRowMapper;
	private final UpstreamReferenceColumnMetadataRowMapper upstreamReferenceColumnMetadataRowMapper;

	
	@Autowired
	public MetadataRepositoryImpl(@Qualifier("sourceDataSource") DataSource dataSource) {
		this(dataSource, new ColumnMetadataRowMapper(), new UpstreamReferenceColumnMetadataRowMapper(), new DownstreamReferenceColumnMetadataRowMapper());
	}

	// TODO use the correct constructor
	public MetadataRepositoryImpl(@Qualifier("sourceDataSource") DataSource dataSource, 
			ColumnMetadataRowMapper columnMetadataRowMapper, 
			UpstreamReferenceColumnMetadataRowMapper upstreamReferenceColumnMetadataRowMapper,
			DownstreamReferenceColumnMetadataRowMapper downstreamReferenceColumnMetadataRowMapper) {
		this.dataSource = dataSource;
		this.columnMetadataRowMapper = columnMetadataRowMapper;
		this.upstreamReferenceColumnMetadataRowMapper = upstreamReferenceColumnMetadataRowMapper;
		this.downstreamReferenceColumnMetadataRowMapper = downstreamReferenceColumnMetadataRowMapper;
	}


	@Override
	public TableMetadata getMetaData(TableNameIdentifier tableNameIdentifier) {
		try(Connection connection = dataSource.getConnection()) {
			DatabaseMetaData metaData = connection.getMetaData();

			List<TableMetadata> tableMetaDataList = Lists.newArrayList();
			ResultSet tableResultSet = metaData.getTables(tableNameIdentifier.getCatalog(), tableNameIdentifier.getSchema(), tableNameIdentifier.getTableName(), new String[] {"TABLE"} );
			while(tableResultSet.next()) {
				String catalog = tableResultSet.getString("TABLE_CAT");
				String schema = tableResultSet.getString("TABLE_SCHEM");
				String tableName = tableResultSet.getString("TABLE_NAME");
				
				TableNameIdentifier tableNameIdenfier = new TableNameIdentifier(catalog, schema, tableName);
				
				List<ColumnMetaData> columns = getColumns(metaData, tableNameIdenfier);
				Map<String, List<DownstreamReferenceColumnMetaData>> downstreamReferenceColumns = getDownstreamReferences(metaData, tableNameIdenfier);
				Map<String, List<UpstreamReferenceColumnMetaData>> upstreamReferenceColumns = getUpstreamReferences(metaData, tableNameIdenfier);
				List<PrimaryKeyMetadata> primaryKeys = getPrimaryKeys(metaData, tableNameIdenfier, columns);
				tableMetaDataList.add(new TableMetadata(tableNameIdenfier, columns, primaryKeys, upstreamReferenceColumns, downstreamReferenceColumns));
			}

			if(tableMetaDataList.isEmpty()) {
				throw new IllegalArgumentException(String.format("Could not find metainformation for %s", tableNameIdentifier));
			}
			
			if(tableMetaDataList.size() > 1) {
				throw new IllegalArgumentException(String.format("Got more than one table on %s", tableNameIdentifier));
			}
			
			return tableMetaDataList.get(0);
		} catch(Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	private List<PrimaryKeyMetadata> getPrimaryKeys(DatabaseMetaData metaData, TableNameIdentifier tableNameIdenfier,
			List<ColumnMetaData> columns) throws SQLException {
		
		Map<String, ColumnMetaData> map = columns.stream()
				.collect(Collectors.toMap(ColumnMetaData::getName, a -> a));
		
		PrimaryKeyMetadataRowMapper mapper = new PrimaryKeyMetadataRowMapper(map);
		
		List<PrimaryKeyMetadata> result = Lists.newArrayList();
		try(ResultSet resultSet = metaData.getPrimaryKeys(tableNameIdenfier.getCatalog(), tableNameIdenfier.getSchema(), tableNameIdenfier.getTableName())) {
			int count = 0;
			while(resultSet.next()) {
				result.add(mapper.mapRow(resultSet, count++));
			}
		}
		return result;
	}

	private Map<String, List<DownstreamReferenceColumnMetaData>> getDownstreamReferences(DatabaseMetaData metaData, TableNameIdentifier tableNameIdenfier) throws SQLException {
		Map<String, List<DownstreamReferenceColumnMetaData>> result = Maps.newHashMap();
		try(ResultSet resultSet = metaData.getExportedKeys(tableNameIdenfier.getCatalog(), tableNameIdenfier.getSchema(), tableNameIdenfier.getTableName())) {
			int count = 0;
			while(resultSet.next()) {
				DownstreamReferenceColumnMetaData downstreamReference = downstreamReferenceColumnMetadataRowMapper.mapRow(resultSet, count++);
				String key = downstreamReference.getForeignConstraintName();
				List<DownstreamReferenceColumnMetaData> list = result.get(key);
				if(list == null) {
					list = Lists.newArrayList();
					result.put(key, list);
				}
				
				list.add(downstreamReference);
			}
		}
		return result;
	}
	
	private Map<String, List<UpstreamReferenceColumnMetaData>> getUpstreamReferences(DatabaseMetaData metaData, TableNameIdentifier tableNameIdenfier) throws SQLException {
		Map<String, List<UpstreamReferenceColumnMetaData>> result = Maps.newHashMap();
		try(ResultSet resultSet = metaData.getImportedKeys(tableNameIdenfier.getCatalog(), tableNameIdenfier.getSchema(), tableNameIdenfier.getTableName())) {
			int count = 0;
			while(resultSet.next()) {
				UpstreamReferenceColumnMetaData upstreamReference = upstreamReferenceColumnMetadataRowMapper.mapRow(resultSet, count++);
				String key = upstreamReference.getForeignConstraintName();
				List<UpstreamReferenceColumnMetaData> list = result.get(key);
				if(list == null) {
					list = Lists.newArrayList();
					result.put(key, list);
				}
				
				list.add(upstreamReference);
			}
		}
		return result;
	}

	private List<ColumnMetaData> getColumns(DatabaseMetaData metaData, TableNameIdentifier tableNameIdenfier)
			throws SQLException {
		List<ColumnMetaData> columns = Lists.newArrayList();
		try(ResultSet columnResultSet = metaData.getColumns(tableNameIdenfier.getCatalog(), tableNameIdenfier.getSchema(), tableNameIdenfier.getTableName() , null)) {
			int count = 0;
			while(columnResultSet.next()) {
				columns.add(columnMetadataRowMapper.mapRow(columnResultSet, count++));
			}
		}
		return columns;
	}
}
