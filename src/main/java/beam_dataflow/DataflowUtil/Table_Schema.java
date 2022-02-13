package beam_dataflow.DataflowUtil;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.io.Serializable;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class Table_Schema implements Serializable 
{	


	private static final long serialVersionUID = 1L;

	@SuppressWarnings({ "rawtypes", "unchecked", "unused" })
	public static TableSchema getTableSchema(String schema_path,String project) throws ParseException 
	{
			String bucket= null;
			String json_path= null;
			bucket =(schema_path.split("gs://")[1]).split("/")[0];
			json_path = schema_path.split("gs://"+bucket+"/")[1];
			Storage storage = StorageOptions.newBuilder().setProjectId(project).build().getService();
			Blob blob = storage.get(bucket,json_path);
			String str= new String(blob.getContent());
			
			
		    StringBuilder str_col = new StringBuilder();
		    ArrayList<String> col_dt = new ArrayList<String>();
			ArrayList<String> col = new ArrayList<String>();
			ArrayList<String> dt = new ArrayList<String>();
			
			try {
				Object obj = new JSONParser().parse(str);
		        JSONObject jsonObject = (JSONObject)obj;
	            JSONObject jo = (JSONObject) obj;			
				JSONArray ja = (JSONArray) jo.get("schema");
				Iterator itr2 = ja.iterator();
				while (itr2.hasNext())
				{
					Iterator<Map.Entry> itr1 = ((Map) itr2.next()).entrySet().iterator();
					while (itr1.hasNext()) {
						  
				        // use add() method to add elements in the list
						
						Map.Entry pair = itr1.next();
						String s=pair.getValue().toString();
						col_dt.add(s);
					}
					
				}
			} catch(Exception e) {
			         e.printStackTrace();
			}
	   
	    
		for(int i=0;i<col_dt.size();i++) {
	        if(i%2==0) {
	            col.add(col_dt.get(i));
	            dt.add(col_dt.get(i+1));
	        }
	            
	    }
	    for (String c : col) {
	        str_col.append(c);
	        str_col.append(",");
	    }
	    str_col.deleteCharAt(str_col.length() - 1);
	        	        
	        
	        List<TableFieldSchema> columns = new ArrayList<TableFieldSchema>();
	        for(int i=0;i<col.size();i++) 
	        {
	            columns.add(new TableFieldSchema().setName(col.get(i)).setType(dt.get(i))); 
	        }
        
        
	        
	        return new TableSchema().setFields(columns);
	    }


}
