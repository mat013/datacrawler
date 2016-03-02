package dk.emstar.data.datadub.repository;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Table;

import dk.emstar.data.datadub.metadata.RowId;
import dk.emstar.data.datadub.metadata.TableData;
import dk.emstar.data.datadub.metadata.TableNameIdentifier;
import dk.emstar.data.datadub.modification.RowAction;

public interface TableDataRepository {

	TableData get(TableNameIdentifier tableIdentifier, Table<RowId, String, Object> keys);

	/**
	 * Get tableIdentifier using the primary key
	 * 
	 * @param tableIdentifier
	 * @param tableData
	 * @return
	 */
	TableData getByPrimaryKey(TableNameIdentifier tableIdentifier, TableData tableData);
	TableData getByReferenceKey(TableNameIdentifier tableIdentifier, TableData tableData);
	TableData getByMirror(TableNameIdentifier tableIdentifier, TableData tableData);
	
	
	TableData get(TableNameIdentifier tableIdentifier, Map<String, String> columnToFieldMap,
			Table<RowId, String, Object> keys);

	void persist(TableData tableData, Map<RowId, RowAction> rowActionResult, TableData destination);

	List<String> getInsertStatements(TableData tableData, Map<RowId, RowAction> rowActionResult, TableData destination);

	TableData findMirrorTable(TableData table);

	void apply(Map<RowId, RowAction> rowActionResult, TableData mirrorTable, TableData sourceTable);

	String getSchema();
	
}
