package dk.emstar.data.datadub.repository;

import dk.emstar.data.datadub.metadata.TableMetadata;
import dk.emstar.data.datadub.metadata.TableNameIdentifier;

public interface MetadataRepository {

	TableMetadata getMetaData(TableNameIdentifier tableNameIdentifier);

}
