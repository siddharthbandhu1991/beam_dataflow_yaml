package beam_dataflow.DataflowTransform;

import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import com.google.api.services.bigquery.model.TableRow;


public class JDBCtoTableRowSource extends DoFn<List<KV<String, String>>, TableRow>  
{

	private static final long serialVersionUID = 1L;

	@ProcessElement
    public void processElement(ProcessContext c) throws Exception 
	{
		List<KV<String, String>> rows = (List<KV<String, String>>) c.element();
		TableRow row = new TableRow();
		for(int i=0;i<rows.size();i++)
		{

         	if(rows.get(i).getValue().equals(""))
         		row.set(rows.get(i).getKey().toString(), null);
        	else
        		row.set(rows.get(i).getKey().toString(),rows.get(i).getValue().toString());		

	}
		//System.out.println(row);
		c.output(row);
    }
}
