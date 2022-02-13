package beam_dataflow.DataflowTransform;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import com.google.api.services.bigquery.model.TableRow;

import beam_dataflow.DataflowUtil.Table_Schema;
import beam_dataflow.DataflowUtil.YamlUtil;



public class JDBCtoTableRow extends DoFn<List<KV<String, String>>, TableRow> 
{

	private static final long serialVersionUID = 1L;
	private String project;
	private String json_file;
	private String sourcesystemcode;
	
	   public JDBCtoTableRow(YamlUtil yamlUtil, String feed_id) throws IOException 
	   {
		   project = yamlUtil.getfeeds(feed_id, "dataflow_project_id");
		   json_file = yamlUtil.getfeeds(feed_id, "dataflow_job_schema_file");
		   sourcesystemcode = yamlUtil.getfeeds(feed_id, "dataflow_csv_source_system_code");
	   }

	@ProcessElement
    public void processElement(ProcessContext c) throws Exception 
	{
		List<KV<String, String>> rows = (List<KV<String, String>>) c.element();
		TableRow row = new TableRow();
		for(int i=0;i<rows.size();i++)
		{
			String col = Table_Schema.getTableSchema(json_file,project).getFields().get(i).getName();
			
         	if(rows.get(i).getValue().equals(""))
         		row.set(col, null);
        	else
        		row.set(col,rows.get(i).getValue().toString());		
         	
			row.set("source_system_code", sourcesystemcode);
			row.set("insert_timestamp", Instant.now().toString());
	}
		//System.out.println(row);
		c.output(row);
    }
}
