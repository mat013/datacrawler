package dk.emstar.data.datadub;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import dk.emstar.data.datadub.fun.RowAction;
import dk.emstar.data.datadub.fun.RowActionSelector;
import dk.emstar.data.datadub.metadata.TableData;
import dk.emstar.data.datadub.metadata.TableNameIdentifier;
import dk.emstar.data.datadub.repository.TableDataRepository;

public interface TableCopier {

	void persist(Map<TableNameIdentifier, TableData> sourceTables,
			Map<TableNameIdentifier, Map<Long, RowAction>> actions, Map<TableNameIdentifier, TableData> mirrorTables,
			TableDataRepository tableDataRepository,
			Function<TableNameIdentifier, TableNameIdentifier> tableIdentifierMapper);

	void apply(Map<TableNameIdentifier, Map<Long, RowAction>> actions, Map<TableNameIdentifier, TableData> sourceTables,
			Map<TableNameIdentifier, TableData> mirrorTables, TableDataRepository tableDataRepository,
			Function<TableNameIdentifier, TableNameIdentifier> tableIdentifierMapper);

	Map<TableNameIdentifier, Map<Long, RowAction>> determineAction(Map<TableNameIdentifier, TableData> sourceTables,
			Map<TableNameIdentifier, TableData> mirrorTables, RowActionSelector rowActionCategorizer,
			Function<TableNameIdentifier, TableNameIdentifier> tableIdentifierMapper);

	Map<TableNameIdentifier, TableData> findMirrors(Map<TableNameIdentifier, TableData> sourceTables,
			TableDataRepository tableDataRepository,
			Function<TableNameIdentifier, TableNameIdentifier> tableIdentifierMapper);

	Map<TableNameIdentifier, TableData> findTableDataAndAssociated(TableData tableDataOrigin,
			Predicate<TableNameIdentifier> whiteListPredicate, TableDataRepository tableDataRepository);

	List<String> getInsertStatements(TableDataRepository tableDataRepository, TableData sourceTable,
			TableData destinationTable, Map<Long, RowAction> actions);

}
