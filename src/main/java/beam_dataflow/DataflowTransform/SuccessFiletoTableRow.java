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
public class SuccessFiletoTableRow extends DoFn<List<JSONObject>, TableRow> {

	// private String project;
	// private String json_file;
	private String sourcesystemcode;
	private JSONObject columnmapping;

	public SuccessFiletoTableRow(YamlUtil yamlUtil, String feed_id) throws IOException, ParseException {
		// project = yamlUtil.getfeeds(feed_id, "dataflow_project_id");
		// json_file = yamlUtil.getfeeds(feed_id, "dataflow_job_schema_file");
		sourcesystemcode = yamlUtil.getfeeds(feed_id, "dataflow_source_system_code");
		JSONParser parser = new JSONParser();
		columnmapping = (JSONObject) parser.parse(yamlUtil.getfeeds(feed_id, "dataflow_columnmapping"));
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@ProcessElement
	public void processElement(ProcessContext c) throws Exception {

		List<JSONObject> lines = c.element();
		
		for (JSONObject line : lines)
		{

		JSONObject success = new JSONObject();
		Object obj = new JSONParser().parse(line.get("success").toString());
		
		try {
		if (!((JSONObject) obj).containsKey("error")) {
			success = (JSONObject) obj;
			Set sourcecol = success.keySet();
			List<String> column = new ArrayList<String>(sourcecol);
			TableRow row = new TableRow();
			for (int i = 0; i < success.size(); i++) {
				String col1 = (String) columnmapping.get(column.get(i).trim());
				row.set(col1, success.get(column.get(i)));
				row.set("source_system_code", sourcesystemcode);
			}
			c.output(row);
		}
		}catch(Exception e)
		{
			
		}
	}
	}
}
