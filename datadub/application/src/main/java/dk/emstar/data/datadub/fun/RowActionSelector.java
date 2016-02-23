package dk.emstar.data.datadub.fun;

import java.util.Map;

import dk.emstar.data.datadub.metadata.TableMetadata;

public interface RowActionSelector {
	RowAction select(Map<String, Object> sourceRow, Map<String, Object> destinationRow, TableMetadata sourceMetaData, TableMetadata destinationMetaData);	
}