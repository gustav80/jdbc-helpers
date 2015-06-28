package no.bekk.java.jdbc;

import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class MetadataAwareRowMapper<T> {

	public abstract T mapRow(ResultSet rs, int rowNum, IndicesForTables metadata) throws SQLException;

	public RowMapper<T> newMapper() {
		return new InitializeMetadataOnFirstRow();
	}

	private class InitializeMetadataOnFirstRow implements RowMapper<T> {

		private Metadata metadata;

		@Override
		public T mapRow(ResultSet rs, int rowNum) throws SQLException {
			if (metadata == null) {
				metadata = new Metadata(rs);
			}
			return MetadataAwareRowMapper.this.mapRow(rs, rowNum, metadata);
		}

	}

}
