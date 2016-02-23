package dk.emstar.data.datadub.metadata;

public interface OnCellChangeSink {

	void onCellChange(ColumnMetaData columnMetaData, Object matchingValue, Object value);

}
