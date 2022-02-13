package beam_dataflow.DataflowUtil;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.yaml.snakeyaml.Yaml;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class YamlUtil implements Serializable 
{

	private static final long serialVersionUID = 1L;
	private String project;
	private String bucket;
	private String config_path;
		
	public YamlUtil(String[] arg)
		{
	    Object obj;
		try {
			obj = new JSONParser().parse(arg[0].replace('\\', '"'));
	        JSONObject jo = (JSONObject) obj;
	        project = (String) jo.get("project_name");
	        bucket = (String) jo.get("bucket_name");
	        config_path = (String) jo.get("config_filename");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		}
	
	public Map<String, Object> readyaml() throws IOException 
	{
		Storage storage = StorageOptions.newBuilder().setProjectId(project).build().getService();
		Blob blob = storage.get(bucket,config_path);
		String str= new String(blob.getContent());
		Yaml yaml = new Yaml();
		Map<String, Object> data = yaml.load(str);
		return data;
	}
	

	@SuppressWarnings({ "rawtypes", "unchecked"})
	public ArrayList getallfeeds() throws IOException 
	{
		Map<String, Object> data = readyaml();
		ArrayList feedlist = (ArrayList) data.get("feeds");
		ArrayList feedname = new ArrayList();
		 for(int i =0;i<feedlist.size();i++) 
		  {
			   String config = (String) ((Map<String,Object>) feedlist.get(i)).get("feed_id");
			   
			   if(config.equals("dataflow")){}
			   else {feedname.add(config);}
		  }
		
		return feedname;
	}

	@SuppressWarnings({ "unchecked", "rawtypes", "unused" })
	public String getfeeds(String feed_id, String key) throws IOException 
	{
		Map<String, Object> data = readyaml();
		ArrayList feedlist = (ArrayList) data.get("feeds");
		String value = new String();
		for(int i=0;i<feedlist.size();i++) 
		  {
			   String config = (String) ((Map<String,Object>) feedlist.get(i)).get("feed_id");
			   if(config.trim().equals(feed_id))
			   {				   
				   Map<String,Object> feed = (Map<String, Object>) feedlist.get(i);
				   System.out.println(key + "  =  " + feed.get(key).toString());
				   value = feed.get(key).toString();
			   }
		  }
		return value;
	}
	
	
}
