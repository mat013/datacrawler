package dk.emstar.data.datadub;

import dk.emstar.data.datadub.repository.TableDataRepository;

public interface TableDubberFactory {

	TableDubber newTableDubber(TableDataRepository source, TableDataRepository destination);
}
