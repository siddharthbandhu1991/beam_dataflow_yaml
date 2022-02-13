package beam_dataflow;

import beam_dataflow.DataflowReader.GCSFileIOReader;
import beam_dataflow.DataflowSetup.DefineProperties;
import beam_dataflow.DataflowTransform.SuccessFiletoTableRow;
import beam_dataflow.DataflowTransform.FailureFiletoTableRow;
import beam_dataflow.DataflowTransform.JDBCtoTableRow;
import beam_dataflow.DataflowTransform.JDBCtoTableRowSource;
import beam_dataflow.DataflowTransform.TableRowtoString;
import beam_dataflow.DataflowUtil.Table_Schema;
import beam_dataflow.DataflowUtil.YamlUtil;
import java.io.IOException;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionList;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import com.google.api.services.bigquery.model.TableRow;

public class main_module implements Serializable {

	private static final long serialVersionUID = 1L;

	public static void main(String[] args) throws IOException, ParseException {
		System.out.println(args[0].replace('\\', '"'));
		YamlUtil yamlUtil = new YamlUtil(args);

		final DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
		DefineProperties.configurePipeLineOptions(options, yamlUtil);
		FileSystems.setDefaultPipelineOptions(options);

		final Pipeline pipeline = Pipeline.create(options);

		@SuppressWarnings("rawtypes")
		ArrayList feeds = yamlUtil.getallfeeds();
		int feedcount = 0;
		while (feeds.size() > feedcount) {
			String[] reader_type = null;
			if (yamlUtil.getfeeds(feeds.get(feedcount).toString(), "dataflow_job_reader").contains(","))
				reader_type = yamlUtil.getfeeds(feeds.get(feedcount).toString(), "dataflow_job_reader").split(",");
			else
				reader_type = new String[] {
						yamlUtil.getfeeds(feeds.get(feedcount).toString(), "dataflow_job_reader") };

			String[] transform_type = null;
			if (yamlUtil.getfeeds(feeds.get(feedcount).toString(), "dataflow_job_transform").contains(","))
				transform_type = yamlUtil.getfeeds(feeds.get(feedcount).toString(), "dataflow_job_transform")
						.split(",");
			else
				transform_type = new String[] {
						yamlUtil.getfeeds(feeds.get(feedcount).toString(), "dataflow_job_transform") };

			String[] writer_type = null;
			if (yamlUtil.getfeeds(feeds.get(feedcount).toString(), "dataflow_job_writer").contains(","))
				writer_type = yamlUtil.getfeeds(feeds.get(feedcount).toString(), "dataflow_job_writer").split(",");
			else
				writer_type = new String[] {
						yamlUtil.getfeeds(feeds.get(feedcount).toString(), "dataflow_job_writer") };

			PCollectionList<List<JSONObject>> lines = null;
			PCollectionList<TableRow> tablerow_success = null;
			PCollectionList<TableRow> tablerow_failure = null;
			PCollectionList<List<KV<String, String>>> jdbcrows = null;

			// Defining Transformation

			int rcount = 0;
			while (reader_type.length > rcount) {
				// Defining Reader Flow
				if (reader_type[rcount].equals("gcs")) {
					String[] file = null;
					if (yamlUtil.getfeeds(feeds.get(feedcount).toString(), "dataflow_job_gcsreadfile").contains(","))
						file = yamlUtil.getfeeds(feeds.get(feedcount).toString(), "dataflow_job_gcsreadfile")
								.split(",");
					else
						file = new String[] {
								yamlUtil.getfeeds(feeds.get(feedcount).toString(), "dataflow_job_gcsreadfile") };

					if (lines == null) {
						lines = PCollectionList.of(pipeline
								.apply(feeds.get(feedcount).toString(), FileIO.match().filepattern(file[rcount]))
								.apply("File_Matching_Pattern", FileIO.readMatches()).apply("Read_Lines",
										ParDo.of(new GCSFileIOReader(yamlUtil, feeds.get(feedcount).toString()))));
					} else {
						lines = lines.and(pipeline
								.apply(feeds.get(feedcount).toString(), FileIO.match().filepattern(file[rcount]))
								.apply("File_Matching_Pattern", FileIO.readMatches()).apply("Read_Lines",
										ParDo.of(new GCSFileIOReader(yamlUtil, feeds.get(feedcount).toString()))));
					}

				}

				if (reader_type[rcount].equals("jdbc")) {
					if (jdbcrows == null) {
						String jdbc_driver = yamlUtil.getfeeds(feeds.get(feedcount).toString(),
								"dataflow_job_server_jdbc_driver");
						String jdbc_url = yamlUtil.getfeeds(feeds.get(feedcount).toString(),
								"dataflow_job_server_jdbc_url");
						String server_username = yamlUtil.getfeeds(feeds.get(feedcount).toString(),
								"dataflow_job_server_username");
						String server_password = yamlUtil.getfeeds(feeds.get(feedcount).toString(),
								"dataflow_job_server_password");
						String server_sql = yamlUtil.getfeeds(feeds.get(feedcount).toString(),
								"dataflow_job_server_sql");
						jdbcrows = PCollectionList
								.of(pipeline
										.apply(feeds.get(feedcount).toString(),JdbcIO.<List<KV<String, String>>>read()
												.withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
														.create(jdbc_driver, jdbc_url).withUsername(server_username)
														.withPassword(server_password))
												.withQuery(server_sql)
												.withCoder(ListCoder
														.of(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
												.withRowMapper(new JdbcIO.RowMapper<List<KV<String, String>>>() {
													private static final long serialVersionUID = 1L;

													@SuppressWarnings("null")
													public List<KV<String, String>> mapRow(ResultSet resultSet)
															throws Exception {
														List<KV<String, String>> addRow = new ArrayList<KV<String, String>>();
														;
														int i = 1;
														while (i <= resultSet.getMetaData().getColumnCount()) {
															if (resultSet.getString(i) == null)
																addRow.add(KV.of(
																		resultSet.getMetaData().getColumnName(i), ""));
															else
																addRow.add(
																		KV.of(resultSet.getMetaData().getColumnName(i),
																				resultSet.getString(i)));
															i++;
														}
														return addRow;
													}
												})));
					}

					else {
						String jdbc_driver = yamlUtil.getfeeds(feeds.get(feedcount).toString(),
								"dataflow_job_server_jdbc_driver");
						String jdbc_url = yamlUtil.getfeeds(feeds.get(feedcount).toString(),
								"dataflow_job_server_jdbc_url");
						String server_username = yamlUtil.getfeeds(feeds.get(feedcount).toString(),
								"dataflow_job_server_username");
						String server_password = yamlUtil.getfeeds(feeds.get(feedcount).toString(),
								"dataflow_job_server_password");
						String server_sql = yamlUtil.getfeeds(feeds.get(feedcount).toString(),
								"dataflow_job_server_sql");
						jdbcrows = jdbcrows
								.and(pipeline
										.apply(feeds.get(feedcount).toString(),JdbcIO.<List<KV<String, String>>>read()
												.withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
														.create(jdbc_driver, jdbc_url).withUsername(server_username)
														.withPassword(server_password))
												.withQuery(server_sql)
												.withCoder(ListCoder
														.of(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
												.withRowMapper(new JdbcIO.RowMapper<List<KV<String, String>>>() {
													private static final long serialVersionUID = 1L;

													@SuppressWarnings("null")
													public List<KV<String, String>> mapRow(ResultSet resultSet)
															throws Exception {
														List<KV<String, String>> addRow = new ArrayList<KV<String, String>>();
														;
														int i = 1;
														while (i <= resultSet.getMetaData().getColumnCount()) {
															addRow.add(KV.of(resultSet.getMetaData().getColumnName(i),
																	resultSet.getString(i)));
															i++;
														}
														return addRow;
													}
												})));
					}
				}
				rcount++;
			}

			// Defining Transformation
			int tcount = 0;
			while (transform_type.length > tcount) {
				if (transform_type[tcount].equals("file2tablerow")) {
					if (tablerow_success == null) {
						tablerow_success = PCollectionList.of(lines.get(tcount).apply("ConvertToBqRow_Success",
								ParDo.of(new SuccessFiletoTableRow(yamlUtil, feeds.get(feedcount).toString()))));
					} else {
						tablerow_success = tablerow_success.and(lines.get(tcount).apply("ConvertToBqRow_Success",
								ParDo.of(new SuccessFiletoTableRow(yamlUtil, feeds.get(feedcount).toString()))));
					}
				
				
					if (tablerow_failure == null) {
						tablerow_failure = PCollectionList.of(lines.get(tcount).apply("ConvertToBqRow_Failure",
								ParDo.of(new FailureFiletoTableRow(yamlUtil, feeds.get(feedcount).toString()))));
					} else {
						tablerow_failure = tablerow_failure.and(lines.get(tcount).apply("ConvertToBqRow_Failure",
								ParDo.of(new FailureFiletoTableRow(yamlUtil, feeds.get(feedcount).toString()))));
					}
				}


				if (transform_type[tcount].equals("jdbc2tablerow")) {

					if (tablerow_success == null) {
						tablerow_success = PCollectionList.of(jdbcrows.get(tcount).apply("JDBC_KVToTableRow",
								ParDo.of(new JDBCtoTableRow(yamlUtil, feeds.get(feedcount).toString()))));
					} else {
						tablerow_success = tablerow_success.and(jdbcrows.get(tcount).apply("JDBC_KVToTableRow",
								ParDo.of(new JDBCtoTableRow(yamlUtil, feeds.get(feedcount).toString()))));
					}
				}

				if (transform_type[tcount].equals("jdbc2tablerow_source")) {

					if (tablerow_success == null) {
						tablerow_success = PCollectionList.of(
								jdbcrows.get(tcount).apply("JDBC_KVToString", ParDo.of(new JDBCtoTableRowSource())));
					} else {
						tablerow_success = tablerow_success.and(
								jdbcrows.get(tcount).apply("JDBC_KVToString", ParDo.of(new JDBCtoTableRowSource())));
					}
				}
				tcount++;
			}

			// Defining Writer Flow
			int dcount = 0;
			int tcount1 = 0;
			while (transform_type.length > tcount1) {
				while (writer_type.length > dcount) {
					if (writer_type[dcount].equals("bq")) {
						String success_json_file_path = yamlUtil.getfeeds(feeds.get(feedcount).toString(),
								"dataflow_job_schema_file");
						
						String project_id = yamlUtil.getfeeds(feeds.get(feedcount).toString(), "dataflow_project_id");
						tablerow_success.get(tcount1).apply("WriteToBQ_Success", BigQueryIO.writeTableRows()
								.to(yamlUtil.getfeeds(feeds.get(feedcount).toString(), "dataflow_job_tablename"))
								.withSchema(Table_Schema.getTableSchema(success_json_file_path, project_id))
								.withWriteDisposition(WriteDisposition.WRITE_APPEND)
								.withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));
						
						String deadletter_json_file_path = yamlUtil.getfeeds(feeds.get(feedcount).toString(),
								"dataflow_job_deadletter_schema_file");
						tablerow_failure.get(tcount1).apply("WriteToBQ_Failure", BigQueryIO.writeTableRows()
								.to(yamlUtil.getfeeds(feeds.get(feedcount).toString(), "dataflow_job_deadletter_table"))
								.withSchema(Table_Schema.getTableSchema(deadletter_json_file_path, project_id))
								.withWriteDisposition(WriteDisposition.WRITE_APPEND)
								.withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));
					}

					if (writer_type[dcount].equals("gcs")) {
						String success_outputfile_path = yamlUtil.getfeeds(feeds.get(feedcount).toString(),
								"dataflow_job_gcswritefile_path");
						String success_outputfile_name = yamlUtil.getfeeds(feeds.get(feedcount).toString(),
								"dataflow_job_gcswritefile_name");
						Timestamp timestamp = new Timestamp(System.currentTimeMillis());
						SimpleDateFormat sdf1 = new SimpleDateFormat("dd.MM.yyyy.HH.mm.ss");
						String insert_time = sdf1.format(timestamp).toString().replace(".", "");

						tablerow_success.get(tcount1).apply("TableRow2String_Success", ParDo.of(new TableRowtoString())).apply(
								"WritetoGCS_Success",
								TextIO.write().to(success_outputfile_path)
										.withShardNameTemplate(success_outputfile_name + '_' + insert_time)
										.withNumShards(1)
										.withSuffix(".json"));
						
						String deadletter_outputfile_path = yamlUtil.getfeeds(feeds.get(feedcount).toString(),
								"dataflow_job_deadletter_gcswritefile_path");
						String deadletter_outputfile_name = yamlUtil.getfeeds(feeds.get(feedcount).toString(),
								"dataflow_job_deadletter_gcswritefile_name");
						Timestamp deadletter_timestamp = new Timestamp(System.currentTimeMillis());
						SimpleDateFormat deadletter_sdf1 = new SimpleDateFormat("dd.MM.yyyy.HH.mm.ss");
						String deadletter_insert_time = deadletter_sdf1.format(deadletter_timestamp).toString().replace(".", "");

						tablerow_failure.get(tcount1).apply("TableRow2String_Failure", ParDo.of(new TableRowtoString())).apply(
								"WritetoGCS_Failure",
								TextIO.write().to(deadletter_outputfile_path)
										.withShardNameTemplate(deadletter_outputfile_name + '_' + deadletter_insert_time)
										.withNumShards(1)
										.withSuffix(".json"));
					}
					dcount++;
				}
				tcount1++;
			}

			feedcount++;
		}

		// pipeline.apply("ReadLines",
		// TextIO.read().from(PropertyUtil.getProperty("dataflow.job.gcsreadfile")))
		// .apply("ConverToBqRow",ParDo.of(new CSVtoTableRow()))
		// .apply("Write",
		// TextIO.write().to(PropertyUtil.getProperty("dataflow.job.gcswritefile")));
		// .apply("WriteToBq", BigQueryIO.writeTableRows()
		// .to(PropertyUtil.getProperty("dataflow.job.tablename"))
		// .withWriteDisposition(WriteDisposition.WRITE_APPEND)
		// .withCreateDisposition(CreateDisposition.CREATE_NEVER));

		pipeline.run().waitUntilFinish();
	}

}