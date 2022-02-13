package beam_dataflow.DataflowTransform;

import beam_dataflow.DataflowUtil.YamlUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.beam.sdk.transforms.DoFn;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.api.services.bigquery.model.TableRow;

@SuppressWarnings("serial")
public class FailureFiletoTableRow extends DoFn<List<JSONObject>, TableRow> {

	// private String project;
	// private String json_file;
	private JSONObject columnmapping;

	public FailureFiletoTableRow(YamlUtil yamlUtil, String feed_id) throws IOException, ParseException {
		// project = yamlUtil.getfeeds(feed_id, "dataflow_project_id");
		// json_file = yamlUtil.getfeeds(feed_id, "dataflow_job_schema_file");

		JSONParser parser = new JSONParser();
		columnmapping = (JSONObject) parser
				.parse(yamlUtil.getfeeds(feed_id, "dataflow_deadletter_columnmapping_mapping"));
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@ProcessElement
	public void processElement(ProcessContext c) throws Exception {

		List<JSONObject> lines = c.element();
		
		for(JSONObject line : lines) {
			
		JSONObject failure = new JSONObject();
		Object obj = new JSONParser().parse(line.get("failure").toString());

		failure = (JSONObject) obj;
		Set sourcecol = failure.keySet();
		List<String> column = new ArrayList<String>(sourcecol);
		if (column.size() > 0) {

			TableRow row = new TableRow();
			for (int i = 0; i < failure.size(); i++) {
				String col1 = (String) columnmapping.get(column.get(i).trim());
				row.set(col1, failure.get(column.get(i)));

			}
			c.output(row);
		}
	}
	}
}
