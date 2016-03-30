package dk.emstar.data.datadub.modification;

import java.util.Map;

import dk.emstar.data.datadub.metadata.TableMetadata;

public interface RowActionSelector {
	RowAction selectAction(Map<String, Object> sourceRow, Map<String, Object> destinationRow, TableMetadata sourceMetaData, TableMetadata destinationMetaData);	
}