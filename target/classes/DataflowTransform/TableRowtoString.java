package beam_dataflow.DataflowTransform;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.beam.sdk.transforms.DoFn;
import org.json.simple.JSONObject;

import com.google.api.services.bigquery.model.TableRow;


@SuppressWarnings("serial")
public class TableRowtoString extends DoFn<TableRow, String> {

	@SuppressWarnings("unchecked")
	@ProcessElement
     public void processElement(ProcessContext c) throws Exception 
	{
		TableRow element = c.element();
		
		Set<String> col = element.keySet();
		List<String> column = new ArrayList<String>(col);
		
		JSONObject jo = new JSONObject();
		
		for(int i=0;i<column.size();i++)
		{
			if(column.get(i) != element.get(column.get(i)))
				jo.put(column.get(i), element.get(column.get(i)));
		}
		c.output(jo.toJSONString());
	}
		
		
}



