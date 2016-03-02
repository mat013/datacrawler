package dk.emstar.data.datadub;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import dk.emstar.data.datadub.metadata.RowId;
import dk.emstar.data.datadub.metadata.TableData;
import dk.emstar.data.datadub.metadata.TableNameIdentifier;
import dk.emstar.data.datadub.modification.RowAction;
import dk.emstar.data.datadub.modification.RowActionSelector;
import dk.emstar.data.datadub.repository.TableDataRepository;

@Component
public class TableDubberImpl implements TableDubber {

	private static final Logger logger = LoggerFactory.getLogger(TableDubberImpl.class);
	
	private final TableDataRepository sourceTableDataRepository;
	private final TableDataRepository destinationTableDataRepository;
	
	@Autowired
	public TableDubberImpl(@Qualifier("sourceTableDataRepository") TableDataRepository sourceTableDataRepository,
			@Qualifier("destinationTableDataRepository")  TableDataRepository destinationTableDataRepository) {
				this.sourceTableDataRepository = sourceTableDataRepository;
				this.destinationTableDataRepository = destinationTableDataRepository;
	}

	
	/**
	 * not done
	 * @param sourceTables
	 * @param actions
	 * @param mirrorTables
	 * @param tableDataRepository
	 * @param tableIdentifierMapper 
	 */
	@Override
	public void persist(Map<TableNameIdentifier, TableData> sourceTables, 
			Map<TableNameIdentifier, Map<RowId, RowAction>> actions, 
			Map<TableNameIdentifier, TableData> mirrorTables, 
			TableDataRepository tableDataRepository, Function<TableNameIdentifier, TableNameIdentifier> tableIdentifierMapper) {

		List<TableData> sortedTables = sortTopological(sourceTables.values());
		for (TableData sourceTable : sortedTables) {
			TableNameIdentifier tableIdentifier = tableIdentifierMapper.apply(sourceTable.getTableIdentifier());
			TableData mirrotTable = mirrorTables.get(tableIdentifier);
			Map<RowId, RowAction> rowActionResult = actions.get(tableIdentifier);
			tableDataRepository.persist(sourceTable, rowActionResult, mirrotTable);
		}
		
	}

	@Override
	public void apply(Map<TableNameIdentifier, Map<RowId, RowAction>> actions,
			Map<TableNameIdentifier, TableData> sourceTables, Map<TableNameIdentifier, TableData> mirrorTables, TableDataRepository tableDataRepository, Function<TableNameIdentifier, TableNameIdentifier> tableIdentifierMapper) {
		List<TableData> sortedTables = sortTopological(sourceTables.values());
		for (TableData sourceTable : sortedTables) {
			TableNameIdentifier sourceTableIdentifier = sourceTable.getTableIdentifier();
			TableNameIdentifier tableIdentifier = tableIdentifierMapper.apply(sourceTableIdentifier);
			logger.info("Retrieving mirror {} for {}", tableIdentifier, sourceTableIdentifier);
			Map<RowId, RowAction> rowActionResult = actions.get(tableIdentifier);
			TableData mirrorTable = mirrorTables.get(tableIdentifier);
			tableDataRepository.apply(rowActionResult, mirrorTable, sourceTable);
		}
	}

	private List<TableData> sortTopological(Collection<TableData> values) {
		List<TableData> result = Lists.newArrayList(values);

		return result;
	}

	@Override
	public Map<TableNameIdentifier, Map<RowId, RowAction>> determineAction(
			Map<TableNameIdentifier, TableData> sourceTables, Map<TableNameIdentifier, TableData> mirrorTables, RowActionSelector rowActionCategorizer, Function<TableNameIdentifier, TableNameIdentifier> tableIdentifierMapper) {
		Map<TableNameIdentifier, Map<RowId, RowAction>> result = Maps.newHashMap();

		for (TableData sourceTable : sourceTables.values()) {
			
			TableNameIdentifier mirrorTableIdentifier = tableIdentifierMapper.apply(sourceTable.getTableIdentifier());
			TableData mirrorTable = mirrorTables.get(mirrorTableIdentifier);
			if(mirrorTable == null) {
				throw new IllegalArgumentException(String.format("Unable to find %s", mirrorTableIdentifier));
			}
			Map<RowId, RowAction> actions = sourceTable.determineRowActions(mirrorTable, rowActionCategorizer);
			result.put(mirrorTableIdentifier, actions);
		}
		
		return result;
	}

	@Override
	public Map<TableNameIdentifier, TableData> findMirrors(Map<TableNameIdentifier, TableData> sourceTables,
			TableDataRepository tableDataRepository, Function<TableNameIdentifier, TableNameIdentifier> tableIdentifierMapper) {
		Map<TableNameIdentifier, TableData> result = Maps.newHashMap();

		for (Entry<TableNameIdentifier, TableData> entry : sourceTables.entrySet()) {
			TableNameIdentifier mirrorTableNameIdentifier = tableIdentifierMapper.apply(entry.getKey());
			TableData tableData = tableDataRepository.getByMirror(mirrorTableNameIdentifier, entry.getValue());
			result.put(mirrorTableNameIdentifier, tableData);
		}
		
		return result;
	}
	
	@Override
	public Map<TableNameIdentifier, TableData> findTableDataAndAssociated(TableData tableDataOrigin, Predicate<TableNameIdentifier> whiteListPredicate, TableDataRepository tableDataRepository) {
		Map<TableNameIdentifier, TableData> result = Maps.newHashMap();
		result.put(tableDataOrigin.getTableIdentifier(), tableDataOrigin);
		
		findDownstreamTables(tableDataOrigin, whiteListPredicate, tableDataRepository, result);
		findUpstreamTables(tableDataOrigin, whiteListPredicate, tableDataRepository, result);

		return result;
	}

	private void findUpstreamTables(TableData tableDataOrigin, Predicate<TableNameIdentifier> whiteListPredicate,
			TableDataRepository tableDataRepository, Map<TableNameIdentifier, TableData> result) {
		Map<TableNameIdentifier, TableData> currentWorkingQueue = Maps.newHashMap();
		for (TableNameIdentifier tableNameIdentifier : tableDataOrigin.getUpstreamTables()) {
			currentWorkingQueue.put(tableNameIdentifier, tableDataOrigin);
		}
		
		while(!currentWorkingQueue.isEmpty()) {
			Entry<TableNameIdentifier, TableData> currentElement = currentWorkingQueue.entrySet().iterator().next();
			TableNameIdentifier tableIdentifier = currentElement.getKey();
			currentWorkingQueue.remove(tableIdentifier);
			logger.info("Discovered {}", tableIdentifier);
			
			TableData currentTable = currentElement.getValue();
			TableData tableData = tableDataRepository.getByPrimaryKey(tableIdentifier, currentTable);
			tableData.adviceCellChange(currentTable);
			result.put(tableData.getTableIdentifier(), tableData);

			for (TableNameIdentifier tableNameIdentifier : tableData.getUpstreamTables()) {
				if(whiteListPredicate.test(tableNameIdentifier)) {
				    currentWorkingQueue.put(tableNameIdentifier, tableData);
				}
			}
		}
	}

	private Map<TableNameIdentifier, TableData> findDownstreamTables(TableData tableDataOrigin,
			Predicate<TableNameIdentifier> whiteListPredicate, TableDataRepository tableDataRepository,
			Map<TableNameIdentifier, TableData> result) {
		Map<TableNameIdentifier, TableData> currentWorkingQueue = Maps.newHashMap();
		for (TableNameIdentifier tableNameIdentifier : tableDataOrigin.getDownstreamTables()) {
			if(whiteListPredicate.test(tableNameIdentifier)) {
				currentWorkingQueue.put(tableNameIdentifier, tableDataOrigin);
			}			
		}
		
		while(!currentWorkingQueue.isEmpty()) {
			Entry<TableNameIdentifier, TableData> currentElement = currentWorkingQueue.entrySet().iterator().next();
			TableNameIdentifier tableIdentifier = currentElement.getKey();
			currentWorkingQueue.remove(tableIdentifier);
			logger.info("Discovered {}", tableIdentifier);
			
			TableData currentTable = currentElement.getValue();
			TableData tableData = tableDataRepository.getByReferenceKey(tableIdentifier, currentTable);
			currentTable.adviceCellChange(tableData);
			result.put(tableData.getTableIdentifier(), tableData);
			
			for (TableNameIdentifier tableNameIdentifier : tableData.getDownstreamTables()) {
				if(whiteListPredicate.test(tableNameIdentifier)) {
				   currentWorkingQueue.put(tableNameIdentifier, tableData);
				}
			}
		}
		return result;
	}

	@Override
	public List<String> getInsertStatements(TableDataRepository tableDataRepository, TableData sourceTable,
			TableData destinationTable, Map<RowId, RowAction> actions) {
		return tableDataRepository.getInsertStatements(sourceTable, actions, destinationTable);
	}

}
