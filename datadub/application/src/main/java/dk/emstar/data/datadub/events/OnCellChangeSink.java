package dk.emstar.data.datadub.events;

import dk.emstar.data.datadub.metadata.ColumnMetaData;

public interface OnCellChangeSink {

	void onCellChange(ColumnMetaData columnMetaData, Object matchingValue, Object value);

}
