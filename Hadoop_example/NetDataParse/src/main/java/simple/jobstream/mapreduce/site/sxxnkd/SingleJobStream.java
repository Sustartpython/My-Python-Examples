package simple.jobstream.mapreduce.site.sxxnkd;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		JobNode Html2XXXXObject = new JobNode("RscParse", defaultRootDir,0, "simple.jobstream.mapreduce.site.sxxnkd.Html2XXXXObject_sxcq");
		Html2XXXXObject.setConfig("inputHdfsPath","/RawData/CQU/sxxnkd/big_json/20190130");
		Html2XXXXObject.setConfig("outputHdfsPath","/RawData/CQU/sxxnkd/latest");
		
		JobNode Std = new JobNode("RscParse", defaultRootDir, 0,"simple.jobstream.mapreduce.site.sxxnkd.Stdsxcq");
		Std.setConfig("inputHdfsPath","/RawData/CQU/sxxnkd/latest");
		Std.setConfig("outputHdfsPath","/RawData/CQU/sxxnkd/std");
		
		result.add(Html2XXXXObject);
		Html2XXXXObject.addChildJob(Std);
		return result;
	}
}
