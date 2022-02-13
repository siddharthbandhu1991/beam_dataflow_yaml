package beam_dataflow.DataflowReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import beam_dataflow.DataflowUtil.Table_Schema;
import beam_dataflow.DataflowUtil.YamlUtil;
import beam_dataflow.UserDefineExeption.UserException;

public class CSVFileReader implements Serializable {

	private YamlUtil feeds;
	private String feed_id;
	String timestamp=Instant.now().toString();
	private static final long serialVersionUID = 1L;

	public CSVFileReader(YamlUtil feed, String feed_ids) throws IOException {
		feeds = feed;
		feed_id = feed_ids;

	}

	@SuppressWarnings("unchecked")
	public List<JSONObject> read(BufferedReader r, String file) throws ParseException, IOException {
		String project = feeds.getfeeds(feed_id, "dataflow_project_id");
		String json_file = feeds.getfeeds(feed_id, "dataflow_job_schema_file");
		char delimiter = feeds.getfeeds(feed_id, "dataflow_csv_delimiter").charAt(0);
		String hasheader = feeds.getfeeds(feed_id, "dataflow_csv_has_header");

		CSVParser parser = new CSVParserBuilder().withSeparator(delimiter).withQuoteChar('"').build();
		CSVReader csvReader = new CSVReaderBuilder(r).withCSVParser(parser).build();
		List<JSONObject> jsonlist = new ArrayList<JSONObject>();

		List<String[]> allData = csvReader.readAll();
		List<String> column = new ArrayList<String>();
		if (hasheader.equals("true")) {
			for (String[] row : allData) {
				if (column.isEmpty())
					for (String cell : row) {
						if (column.isEmpty())
							column.add(cell.substring(1));
						else
							column.add(cell);
					}
				break;
			}
		} else {
			try {
				List<TableFieldSchema> col = Table_Schema.getTableSchema(json_file, project).getFields();
				for (int i = 0; i < col.size(); i++)
					if (!col.get(i).getName().equals("sys_information_dict")
							|| !col.get(i).getName().equals("source_system_code")
							|| !col.get(i).getName().equals("insert_timestamp"))
						column.add(col.get(i).getName());
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		int rownum = 0;
		int count = 0;
		for (String[] row : allData) {
			JSONObject jo = new JSONObject();
			JSONObject error_rec = new JSONObject();
			JSONObject success_error = new JSONObject();
			int i = 0;
			rownum++;
			if (count != 0 && hasheader.equals("true")) {
				int m = 1;
				for (String cell : row) {
					if (column.size() != row.length) {
						try {
							if (i < column.size()) {
								jo.put(column.get(i), cell);
							} else {
								jo.put("missing_column_" + m, cell);
								m++;
							}
							jo.put("error", "");
							throw new UserException(row, rownum, file, timestamp);
						}

						catch (UserException e) {
							JSONParser Jparser = new JSONParser();
							error_rec = (JSONObject) Jparser.parse(e.JSONError());
						} catch (Exception e) {
							jo.put("error", "");
							error_rec.put("row_number", String.valueOf(rownum));
							error_rec.put("insert_timestamp", timestamp);
							error_rec.put("error_type", e.toString());
							error_rec.put("payload", Arrays.toString(row));
							error_rec.put("sys_information_dict", "{'file_name':'" + file + "'}");
						}
					} else if (!column.get(i).equals("") && !cell.equals("") && column.size() == row.length) {
						jo.put(column.get(i), cell);
					}

					i++;
				}
				jo.put("insert_timestamp", timestamp);
				jo.put("sys_information_dict", "{'file_name':'" + file + "'}");
				success_error.put("success", jo.toString());
				success_error.put("failure", error_rec.toString());
				jsonlist.add(success_error);
			}

			else if (hasheader.equals("false")) {
				for (String cell : row) {
					try {
						if (!column.get(i).equals("") && !cell.equals("")) {
							jo.put(column.get(i), cell);
						} else {
							try {
								if (column.get(i).equals("") && !cell.equals(""))
									jo.put("error", "");
								throw new UserException(row, rownum, file, timestamp);
							} catch (UserException e) {
								JSONParser Jparser = new JSONParser();
								error_rec = (JSONObject) Jparser.parse(e.JSONError());
							}
						}
						i++;
					} catch (Exception e) {
						jo.put("error", "");
						error_rec.put("row_number", String.valueOf(rownum));
						error_rec.put("insert_timestamp", timestamp);
						error_rec.put("error_type", e.toString());
						error_rec.put("payload", Arrays.toString(row));
						error_rec.put("sys_information_dict", "{'file_name':'" + file + "'}");
					}
					jo.put("insert_timestamp", timestamp);
					jo.put("sys_information_dict", "{'file_name':'" + file + "'}");
					success_error.put("success", jo.toString());
					success_error.put("failure", error_rec.toString());
					// System.out.print(success_error);
					jsonlist.add(success_error);
					// c.output(success_error);
				}
			}
			count++;
		}

		return jsonlist;

	}

}
