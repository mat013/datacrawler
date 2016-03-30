package dk.emstar.data.datadub;

import org.springframework.stereotype.Component;

import dk.emstar.data.datadub.repository.TableDataRepository;

@Component
public class TableDubberFactoryImpl implements TableDubberFactory {

    @Override
    public TableDubber newTableDubber(TableDataRepository source, TableDataRepository destination) {
        return new TableDubberImpl(source, destination);
    }

}
