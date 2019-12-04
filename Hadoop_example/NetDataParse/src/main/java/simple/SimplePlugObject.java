package simple;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;

import simple.jobstream.JobStreamSchedule;

import com.process.frame.AnalysisPlugBase;
import com.process.frame.JobStreamImpl;

public class SimplePlugObject extends AnalysisPlugBase
{

	@Override
	public List<JobStreamImpl> getPlugJobStreamImpl()
	{
		// TODO Auto-generated method stub
		ArrayList<JobStreamImpl> jobStreamimps = new ArrayList<JobStreamImpl>();
		jobStreamimps.add(new JobStreamSchedule());
		return jobStreamimps;
	}

	@Override
	public Options getPlugOptionExt()
	{
		Options opt = new Options();
		return opt;
	}

	@Override
	public boolean executeExtCommand(CommandLine cmd, Configuration conf)
	{
		return false;
	}

}
