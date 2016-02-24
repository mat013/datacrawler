package dk.emstar.data.datadub.modification;

public enum RowAction {
	COPY_ROW_RETAIN_KEY,
	COPY_ROW_CHANGE_KEY,
	IGNORE,
	USE_EXISTING_ROW_LOOKUP_KEY,
	;		
}