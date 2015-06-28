package no.bekk.java.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class Metadata implements IndicesForTables {
	private final Map<String, Indices> tablesToIndices = new HashMap<>();

	public Metadata(ResultSet rs) throws SQLException {
		Map<String, Map<String, Integer>> tablesToColumns = new HashMap<>();

		ResultSetMetaData metadata = rs.getMetaData();
		for (int col = 1; col <= metadata.getColumnCount(); col++) {
			String tableName = metadata.getTableName(col);
			String columnName = metadata.getColumnLabel(col);

			tablesToColumns.putIfAbsent(tableName, new HashMap<>());
			if (tablesToColumns.get(tableName).containsKey(columnName)) {
				throw new RuntimeException("Resultset contains duplicate table-column pair. Not allowed. Fix query. " +
						"(" + tableName + ":" + columnName + "). Metadata contains: " + metadataAsString(metadata));
			}
			tablesToColumns.get(tableName).put(columnName, col);
		}

		tablesToColumns.forEach((key, columns) -> tablesToIndices.put(key, new Indices(columns, key)));
	}

	private String metadataAsString(ResultSetMetaData metadata) throws SQLException {
		StringBuilder sb = new StringBuilder();
		for (int col = 1; col <= metadata.getColumnCount(); col++) {
			String tableName = metadata.getTableName(col);
			String columnName = metadata.getColumnLabel(col);
			sb.append(tableName + ":" + columnName).append(",");
		}
		return sb.toString();
	}

	@Override
	public Indices forTable(String tableNameNotNormalized) {
		String tableName = tableNameNotNormalized.toUpperCase();
		Indices columns = tablesToIndices.get(tableName);
		if (columns == null) {
			String known = tablesToIndices.keySet().stream().collect(Collectors.joining(","));
			throw new RuntimeException("No known columns for table '" + tableName + "'. Known tables: " + known);
		}

		return columns;
	}

}
