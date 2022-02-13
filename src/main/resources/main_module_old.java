package beam_dataflow;
import beam_dataflow.DataflowReader.GCSFileIOReader;
import beam_dataflow.DataflowSetup.DefineProperties;
import beam_dataflow.DataflowTransform.CSVtoTableRow;
import beam_dataflow.DataflowTransform.TableRowColumnAdd;
import beam_dataflow.DataflowUtil.PropertyUtil;
import beam_dataflow.DataflowUtil.Table_Schema;
import beam_dataflow.DataflowUtil.YamlUtil;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionList;
import org.json.simple.parser.ParseException;
import com.google.api.services.bigquery.model.TableRow;

public class main_module_old{

	public static void main(String[] args) throws IOException, ParseException
	{
		System.out.println(args[0].replace('\\', '"'));
		YamlUtil yamlUtil = new YamlUtil(args);
		
		System.out.println(yamlUtil.getfeeds("dataflow", "desc"));
		PropertyUtil propertyUtil = new PropertyUtil(args);

		final DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
		DefineProperties.configurePipeLineOptions(options, propertyUtil);
		FileSystems.setDefaultPipelineOptions(options);

	  	final Pipeline pipeline = Pipeline.create(options);
	  		
	  		String[] reader_type = null;
			if(propertyUtil.getProperty("dataflow.job.reader").contains(","))
				reader_type = propertyUtil.getProperty("dataflow.job.reader").split(",");
			else 
				reader_type = new String[] {propertyUtil.getProperty("dataflow.job.reader")};
				
		  	String[] file = null;
			if(propertyUtil.getProperty("dataflow.job.gcsreadfile").contains(","))
				file = propertyUtil.getProperty("dataflow.job.gcsreadfile").split(",");
			else 
				file = new String[] {propertyUtil.getProperty("dataflow.job.gcsreadfile")};	
				
		  	String[] transform_type = null;
			if(propertyUtil.getProperty("dataflow.job.transform").contains(","))
				transform_type = propertyUtil.getProperty("dataflow.job.transform").split(",");
			else 
				transform_type = new String[] {propertyUtil.getProperty("dataflow.job.transform")};	
				
		  	String[] writer_type = null;
			if(propertyUtil.getProperty("dataflow.job.writer").contains(","))
				writer_type = propertyUtil.getProperty("dataflow.job.writer").split(",");
			else 
				writer_type = new String[] {propertyUtil.getProperty("dataflow.job.writer")};	
			
				
		PCollectionList<String> lines = null;
		PCollectionList<TableRow> tablerow = null;
				
		//Defining Transformation
		
		int rcount = 0;
		while(reader_type.length > rcount)		
		{
		//Defining Reader Flow
			if( reader_type[rcount].equals("gcs"))
			{	

				if (lines == null)
				{
				lines = PCollectionList.of(pipeline.apply("Reading_Files", FileIO.match().filepattern(file[rcount]))
							   		.apply("File_Matching_Pattern", FileIO.readMatches())
							   		.apply("Read_Lines", ParDo.of(new GCSFileIOReader(propertyUtil))));
				}
				else 
				{
				lines = lines.and(pipeline.apply("Reading_Files", FileIO.match().filepattern(file[rcount]))
					   				.apply("File_Matching_Pattern", FileIO.readMatches())
					   				.apply("Read_Lines", ParDo.of(new GCSFileIOReader(propertyUtil))));
				}
			}
			
		
		//Defining Transformation
		int tcount = 0;
		while(transform_type.length > tcount)		
		{
			if( transform_type[tcount].equals("csv2bq"))
				{
				if (tablerow == null)
				{
					tablerow = PCollectionList.of(lines.get(rcount).apply("ConvertToBqRow",ParDo.of(new CSVtoTableRow(propertyUtil)))
									.apply("BqRowColumnAdd",ParDo.of(new TableRowColumnAdd(propertyUtil))));
				}
				else 
				{
					tablerow = tablerow.and(lines.get(rcount).apply("ConvertToBqRow",ParDo.of(new CSVtoTableRow(propertyUtil)))
									.apply("BqRowColumnAdd",ParDo.of(new TableRowColumnAdd(propertyUtil))));
				}							
				}
			tcount++;
			}	

		
		//Defining Writer Flow
		int dcount = 0;
		while(writer_type.length > dcount)		
		{
		if (writer_type[dcount].equals("bq"))
		 	{
			String json_file_path = propertyUtil.getProperty("dataflow.job.schema.file");
			String project_id = propertyUtil.getProperty("dataflow.job.project.id");
			tablerow.get(rcount).apply("WriteToBQ", BigQueryIO.writeTableRows()
						.to(propertyUtil.getProperty("dataflow.job.tablename"))
						.withSchema(Table_Schema.getTableSchema(json_file_path,project_id))
						.withWriteDisposition(WriteDisposition.WRITE_APPEND)
						.withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));
		 	
		 	}
		
		if (writer_type[dcount].equals("gcs"))
	 		{
			lines.get(rcount).apply("WritetoGCS", TextIO.write().to(propertyUtil.getProperty("dataflow.job.gcswritefile")));
	 		}
	
		dcount++;
		}	
		
	rcount++;
	}
	
	  	//pipeline.apply("ReadLines", TextIO.read().from(PropertyUtil.getProperty("dataflow.job.gcsreadfile")))
			//.apply("ConverToBqRow",ParDo.of(new CSVtoTableRow()))
			//.apply("Write", TextIO.write().to(PropertyUtil.getProperty("dataflow.job.gcswritefile")));
        //.apply("WriteToBq", BigQueryIO.writeTableRows()
              //.to(PropertyUtil.getProperty("dataflow.job.tablename"))
               // .withWriteDisposition(WriteDisposition.WRITE_APPEND)
               //.withCreateDisposition(CreateDisposition.CREATE_NEVER));
	  	
	  	
	  	pipeline.run().waitUntilFinish();
  	}
  
}