package beam_dataflow.DataflowReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.util.List;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.transforms.DoFn;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import beam_dataflow.DataflowUtil.YamlUtil;

public class GCSFileIOReader extends DoFn<ReadableFile, List<JSONObject>> {

	private YamlUtil feeds;
	private String feed_id;

	private static final long serialVersionUID = 1L;

	public GCSFileIOReader(YamlUtil feed, String feed_ids) throws IOException {
		feeds = feed;
		feed_id = feed_ids;

	}

	@ProcessElement
	public void process(ProcessContext c) throws IOException, ParseException {
		ReadableFile f = c.element();
		String[] filename = f.getMetadata().resourceId().toString().replace("/", ",").split(",");
		String file = filename[filename.length - 1];

		if (file.substring(file.indexOf("."), file.length()).equals(".csv")) {
			BufferedReader r = new BufferedReader(
					new InputStreamReader(Channels.newInputStream(c.element().open()), "UTF-8"));
			CSVFileReader cvr = new CSVFileReader(feeds, feed_id);
			List<JSONObject> success_error = cvr.read(r, file);
			c.output(success_error);
		}

		if (file.substring(file.indexOf("."), file.length()).equals(".xlsx")) {
			String xlmelt = feeds.getfeeds(feed_id, "dataflow_xl_melt");
			XSSFWorkbook wb = new XSSFWorkbook(Channels.newInputStream(c.element().open()));

			if (xlmelt.toLowerCase().equals("no")) {
				XLSXFileReader xlsx_reader = new XLSXFileReader(feeds, feed_id);
				List<JSONObject> success_error = xlsx_reader.read(wb, file);
				c.output(success_error);
			} else if (xlmelt.toLowerCase().equals("yes")) {
				XLSXFileReader xlsx_reader = new XLSXFileReader(feeds, feed_id);
				List<JSONObject> success_error = xlsx_reader.melt(wb, file);
				c.output(success_error);
			}
		}

		if (file.substring(file.indexOf("."), file.length()).equals(".xls")) {
			String xlmelt = feeds.getfeeds(feed_id, "dataflow_xl_melt");
			HSSFWorkbook wb = new HSSFWorkbook(Channels.newInputStream(c.element().open()));

			if (xlmelt.toLowerCase().equals("no")) {
				XLSFileReader xlsx_reader = new XLSFileReader(feeds, feed_id);
				List<JSONObject> success_error = xlsx_reader.read(wb, file);
				c.output(success_error);
			} else if (xlmelt.toLowerCase().equals("yes")) {
				XLSFileReader xlsx_reader = new XLSFileReader(feeds, feed_id);
				List<JSONObject> success_error = xlsx_reader.melt(wb, file);
				c.output(success_error);
			}
		}
	}

}