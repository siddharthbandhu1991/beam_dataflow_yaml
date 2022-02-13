package beam_dataflow.DataflowUtil;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.util.Properties;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.CharSource;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.Blob;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;


public final class PropertyUtil implements Serializable 
{

	private static final long serialVersionUID = 1L;
	private String project;
	private String bucket;
	private String config_path;
		
	public PropertyUtil(String[] arg)
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
	

	@SuppressWarnings("null")
	public String getProperty(String key) throws IOException 
	{
		
		Properties properties = null;
		String value = null;

		try {
		    properties = new Properties();

			Storage storage = StorageOptions.newBuilder().setProjectId(project).build().getService();
			Blob blob = storage.get(bucket,config_path);
			String str= new String(blob.getContent());
			Reader reader = CharSource.wrap(str).openStream();
		    
		if (reader != null) 
		    {
		        properties.load(reader);
		        value = properties.getProperty(key);
		    }

		} catch (IOException e) {
		    e.printStackTrace();
		}
		
		System.out.println(key + "  =  " + value);
		return value;

	}
}


