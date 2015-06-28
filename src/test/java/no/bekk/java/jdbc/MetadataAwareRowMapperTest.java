package no.bekk.java.jdbc;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class MetadataAwareRowMapperTest {

	private static DataSource datasource;
	private static JdbcTemplate jdbcTemplate;
	@ClassRule
	public static ExternalResource resource = new ExternalResource() {
		@Override
		protected void before() throws Throwable {
			datasource = new DriverManagerDataSource("jdbc:hsqldb:mem:test", "sa", "");
			jdbcTemplate = new JdbcTemplate(datasource);

			jdbcTemplate.update(
					"create table table_a(\n" +
							"  id INT, \n" +
							"  name VARCHAR(255) not null)");

			jdbcTemplate.execute(
					"create table table_b(\n" +
							"  id INT, \n" +
							"  name VARCHAR(255) not null,\n" +
							"  a_id INT not null)");

			jdbcTemplate.update("insert into table_a (id, name) values (1, 'a')");
			jdbcTemplate.update("insert into table_b (id, name, a_id) values (1, 'b', 1)");
		}
	};


	@Test
	public void should_get_column_with_same_name_from_same_resultset_using_different_tables() {
		MetadataAwareRowMapper<TableAandB> combinedRowMapper = new MetadataAwareRowMapper<TableAandB>() {

			@Override
			public TableAandB mapRow(ResultSet rs, int rowNum, IndicesForTables indices) throws SQLException {
				TableA tableA = MAPPER_TABLE_A.mapRow(rs, rowNum, indices);
				TableB tableB = MAPPER_TABLE_B.mapRow(rs, rowNum, indices);
				return new TableAandB(tableA, tableB);
			}
		};

		List<TableAandB> result = jdbcTemplate.query(
				"select a.name as a_name, b.name as b_name, a.*, b.* \n" +
						"from table_a a\n" +
						"join table_b b on b.a_id = a.id;", combinedRowMapper.newMapper());


		TableAandB first = result.iterator().next();
		assertThat(first.tableA.name, is("a"));
		assertThat(first.tableB.name, is("b"));
		System.out.println(result);

	}

	private static MetadataAwareRowMapper<TableA> MAPPER_TABLE_A = new MetadataAwareRowMapper<TableA>() {
		@Override
		public TableA mapRow(ResultSet rs, int rowNum, IndicesForTables indices) throws SQLException {
			Indices tableA = indices.forTable("table_a");

			String nameA = rs.getString(tableA.forColumn("name"));

			return new TableA(nameA);
		}
	};

	private static MetadataAwareRowMapper<TableB> MAPPER_TABLE_B = new MetadataAwareRowMapper<TableB>() {
		@Override
		public TableB mapRow(ResultSet rs, int rowNum, IndicesForTables indices) throws SQLException {
			Indices tableB = indices.forTable("table_b");

			String nameB = rs.getString(tableB.forColumn("name"));

			return new TableB(nameB);
		}
	};


	private static class TableA {
		private final String name;

		public TableA(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return "TableA{" +
					"name='" + name + '\'' +
					'}';
		}
	}

	private static class TableB {
		private final String name;

		public TableB(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return "TableB{" +
					"name='" + name + '\'' +
					'}';
		}
	}

	private static class TableAandB {
		private final TableA tableA;
		private final TableB tableB;

		public TableAandB(TableA tableA, TableB tableB) {
			this.tableA = tableA;
			this.tableB = tableB;
		}

		@Override
		public String toString() {
			return "TableAandB{" +
					"tableA=" + tableA +
					", tableB=" + tableB +
					'}';
		}
	}

}
