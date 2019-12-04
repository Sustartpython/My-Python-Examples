package simple.jobstream.mapreduce.user.walker;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";

		JobNode SimpleJob = new JobNode("SimpleJob", defaultRootDir, 0,
				"simple.jobstream.mapreduce.common.vip.Sqlite2HDFS");
		
//		SimpleJob.setConfig("inputHdfsPath", "/RawData/cnki/qk/detail/latest");
//		SimpleJob.setConfig("outputHdfsPath", "/vipuser/walker/output/tmp20180418");

		result.add(SimpleJob);

		return result;
	}
}
