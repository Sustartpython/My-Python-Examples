package simple.jobstream.mapreduce.site.rscbook;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		JobNode Html2XXXXObject = new JobNode("RscParse", defaultRootDir,0, "simple.jobstream.mapreduce.site.rscbook.Html2xxxxobject");
		Html2XXXXObject.setConfig("inputHdfsPath","/RawData/CQU/rscbook/big_htm/big_htm_20181218");
		Html2XXXXObject.setConfig("outputHdfsPath","/RawData/CQU/rscbook/latest");
		
		JobNode Std = new JobNode("RscParse", defaultRootDir, 0,"simple.jobstream.mapreduce.site.rscbook.StdXXXXObject");
		Std.setConfig("inputHdfsPath","/RawData/CQU/rscbook/latest");
		Std.setConfig("outputHdfsPath","/RawData/CQU/rscbook/std");
		
		result.add(Html2XXXXObject);
		Html2XXXXObject.addChildJob(Std);
		return result;
	}
}
