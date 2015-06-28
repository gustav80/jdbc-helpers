package no.bekk.java.jdbc;

import java.util.Map;
import java.util.stream.Collectors;

public class Indices {

	private final Map<String, Integer> columns;
	private final String tableName;

	public Indices(Map<String, Integer> columns, String tableName) {
		this.columns = columns;
		this.tableName = tableName;
	}

	public Integer forColumn(String columnNameNotNormalized) {
		String columnName = columnNameNotNormalized.toUpperCase();
		Integer columnIndex = columns.get(columnName);
		if (columnIndex == null) {
			String known = columns.keySet().stream().collect(Collectors.joining(", "));
			throw new RuntimeException("Could not find column '" + columnName + "' for table '" + tableName + "' in resultset. Known columns for table: " + known);
		}
		return columnIndex;
	}
}
