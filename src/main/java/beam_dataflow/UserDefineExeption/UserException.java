package beam_dataflow.UserDefineExeption;

import java.util.Arrays;

import org.json.simple.JSONObject;

public class UserException extends Exception{

	private static final long serialVersionUID = 1L;
	String file;
	int rownum;
	String[] payload;
	String timestamp;
	public UserException(String[] payload, int rownum, String file,String timestamp) {
		this.file=file;
		this.rownum=rownum;
		this.payload=payload;
		this.timestamp=timestamp;
	}
	@SuppressWarnings("unchecked")
	public String JSONError(){
		//JSONObject error = jo;
		JSONObject error_rec = new JSONObject();
		//error.put("error", "");
		error_rec.put("row_number", String.valueOf(rownum));
		error_rec.put("insert_timestamp", timestamp);
		error_rec.put("error_type", "Column Mismatch");
		error_rec.put("payload", Arrays.toString(payload));
		error_rec.put("sys_information_dict", "{'file_name':'" + file + "'}");	
	return error_rec.toJSONString();
	}
	}