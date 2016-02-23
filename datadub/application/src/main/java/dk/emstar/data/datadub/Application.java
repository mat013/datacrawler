package dk.emstar.data.datadub;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.Import;

import com.google.common.base.Joiner;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import dk.emstar.data.datadub.configuration.SpringConfig;
import dk.emstar.data.datadub.fun.RowAction;
import dk.emstar.data.datadub.fun.RowActionSelector;
import dk.emstar.data.datadub.metadata.TableData;
import dk.emstar.data.datadub.metadata.TableMetadata;
import dk.emstar.data.datadub.metadata.TableNameIdentifier;
import dk.emstar.data.datadub.repository.TableDataRepository;

@Import({SpringConfig.class})
public class Application implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(Application.class);
	
	private final TableDataRepository sourceTableDataRepository;
	private final TableDataRepository destinationTableDataRepository;
	private final TableCopier tableCopier;

	@Autowired
	public Application(@Qualifier("sourceTableDataRepository") TableDataRepository sourceTableDataRepository,
			@Qualifier("destinationTableDataRepository")  TableDataRepository destinationTableDataRepository,
			TableCopier tableCopier) {
		this.sourceTableDataRepository = sourceTableDataRepository;
		this.destinationTableDataRepository = destinationTableDataRepository;
		this.tableCopier = tableCopier;
	}

	@Override
	public void run(String... args) {
		TableNameIdentifier tableIdentifier = new TableNameIdentifier("MAT013", "ORDER"); 
		
		Table<Long, String, Object> keys = HashBasedTable.create();
		keys.put((long) keys.size(), "ID", 1);
		keys.put((long) keys.size(), "ID", 2);

		TableData sourceTable = sourceTableDataRepository.get(tableIdentifier, keys);

		Function<TableNameIdentifier, TableNameIdentifier> sourceToMirrorMapper = o -> new TableNameIdentifier(destinationTableDataRepository.getSchema(), o.getTableName());
		
		Predicate<TableNameIdentifier> whitelistPredicate = o -> true;

		Map<TableNameIdentifier, TableData> sourceTables = tableCopier.findTableDataAndAssociated(sourceTable, whitelistPredicate, sourceTableDataRepository);
		Map<TableNameIdentifier, TableData> mirrorTables = tableCopier.findMirrors(sourceTables, destinationTableDataRepository, sourceToMirrorMapper);

		RowActionSelector rowActionCategorizer = new RowActionSelector() {
			@Override
			public RowAction select(Map<String, Object> sourceRow, Map<String, Object> destinationRow,
					TableMetadata sourceMetaData, TableMetadata destinationMetaData) {
				return destinationRow == null ? RowAction.CREATE : RowAction.UNACCEPTED;
			}
		};

		Map<TableNameIdentifier, Map<Long, RowAction>> actions = tableCopier.determineAction(sourceTables, mirrorTables, rowActionCategorizer, sourceToMirrorMapper);
		tableCopier.apply(actions, sourceTables, mirrorTables, destinationTableDataRepository, sourceToMirrorMapper);
		for (TableData sourceTable1 : sourceTables.values()) {
			logger.info("{}\r\n{}", sourceTable1.getTableIdentifier(), sourceTable1.toContent());
		}
		
		for (TableData sourceTable1 : sourceTables.values()) {
			TableNameIdentifier mirrorTableIdentifier = sourceToMirrorMapper.apply(sourceTable1.getTableIdentifier());
			TableData mirrorTableData = mirrorTables.get(mirrorTableIdentifier);
			Map<Long, RowAction> actionList = actions.get(mirrorTableIdentifier);
			
			logger.info("{}\r\n{}", sourceTable1.getTableIdentifier(), 
					Joiner.on("\r\n").join(tableCopier.getInsertStatements(destinationTableDataRepository, sourceTable1, mirrorTableData, actionList)));
		}

		tableCopier.persist(sourceTables, actions, mirrorTables, destinationTableDataRepository, sourceToMirrorMapper);
		
	}

	public static void main(String[] args) throws Exception {
		SpringApplication.run(Application.class, args);
	}	
}
