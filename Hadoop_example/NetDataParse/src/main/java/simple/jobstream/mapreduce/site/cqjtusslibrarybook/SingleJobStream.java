package simple.jobstream.mapreduce.site.cqjtusslibrarybook;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";

		String rawHtmlDir = "/RawData/chaoxing/cqjtusslibrarybook/big_json/2018/20181015";
		String rawXXXXObjectDir = "/RawData/chaoxing/cqjtusslibrarybook/XXXXObject";
		String latest_tempDir = "/RawData/chaoxing/cqjtusslibrarybook/latest_temp"; 
		String latestDir = "/RawData/chaoxing/cqjtusslibrarybook/latest"; 
		String newXXXXObjectDir = "/RawData/chaoxing/cqjtusslibrarybook/new_data/XXXXObject";
		String stdDir = "/RawData/chaoxing/cqjtusslibrarybook/new_data/StdDir";

		JobNode Html2XXXXObject = new JobNode("cqjtusslibrarybook", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.cqjtusslibrarybook.Html2XXXXObject");
		Html2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Html2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);

		
		JobNode MergeXXXXObject2Temp = new JobNode("cqjtusslibrarybook", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.cqjtusslibrarybook.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);

		
		JobNode GenNewData = new JobNode("cqjtusslibrarybook", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.cqjtusslibrarybook.GenNewData");
		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);

		JobNode Std = new JobNode("cqjtusslibrarybook", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.cqjtusslibrarybook.StdCqjtu");
		Std.setConfig("inputHdfsPath", newXXXXObjectDir);
		Std.setConfig("outputHdfsPath", stdDir);

		
		JobNode Temp2Latest = new JobNode("cqjtusslibrarybook", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.cqjtusslibrarybook.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);


//		result.add(Html2XXXXObject);
//		Html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
//		MergeXXXXObject2Temp.addChildJob(GenNewData);
//		GenNewData.addChildJob(Std);
		result.add(Std);
		Std.addChildJob(Temp2Latest);

		
		return result;
	}
}
