package beam_dataflow.DataflowSetup;
import beam_dataflow.DataflowUtil.YamlUtil;
import java.io.IOException;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

public final class DefineProperties 
{
	
	public static void configurePipeLineOptions(final DataflowPipelineOptions options, final YamlUtil yamlUtil) throws IOException
	{
		
		options.setProject(yamlUtil.getfeeds("dataflow", "dataflow_project_id"));
		options.setJobName(yamlUtil.getfeeds("dataflow", "dataflow_job_name"));
		
		options.setTempLocation(yamlUtil.getfeeds("dataflow", "dataflow_temp_location"));
		options.setStagingLocation(yamlUtil.getfeeds("dataflow", "dataflow_staging_location"));
		
		options.setMaxNumWorkers(Integer.parseInt(yamlUtil.getfeeds("dataflow", "dataflow_job_max_worker")));
		options.setWorkerMachineType(yamlUtil.getfeeds("dataflow", "dataflow_job_worker_machine_type"));
		options.setRegion(yamlUtil.getfeeds("dataflow", "dataflow_job_region"));
		
		if(yamlUtil.getfeeds("dataflow", "dataflow_job_publicip_use").toLowerCase().equals("false"))
			options.setUsePublicIps(false);
		
		if(yamlUtil.getfeeds("dataflow", "dataflow_job_subnetwork_enable").toLowerCase().equals("true"))
			options.setNetwork(yamlUtil.getfeeds("dataflow", "dataflow_job_subnetwork_vpc"));
		
		options.setServiceAccount(yamlUtil.getfeeds("dataflow", "dataflow_job_service_account"));
						
//		options.setRunner(DataflowRunner.class);

	}

}
