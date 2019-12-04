package simple.jobstream.mapreduce.site.lzufesslibrary;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";

		String rawHtmlDir = "/RawData/chaoxing/lzufesslibrarybook/big_json/20181112";
		String rawXXXXObjectDir = "/RawData/chaoxing/lzufesslibrarybook/XXXXObject";
		String latest_tempDir = "/RawData/chaoxing/lzufesslibrarybook/latest_temp"; 
		String latestDir = "/RawData/chaoxing/lzufesslibrarybook/latest"; 
		String newXXXXObjectDir = "/RawData/chaoxing/lzufesslibrarybook/new_data/XXXXObject";
		String stdDir = "/RawData/chaoxing/lzufesslibrarybook/new_data/StdDir";

		JobNode Html2XXXXObject = new JobNode("lzufesslibrarybook", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.lzufesslibrary.Html2XXXXObject");
		Html2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Html2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);

		
		JobNode MergeXXXXObject2Temp = new JobNode("lzufesslibrarybook", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.lzufesslibrary.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);

		
		JobNode GenNewData = new JobNode("lzufesslibrarybook", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.lzufesslibrary.GenNewData");
		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);

		JobNode Std = new JobNode("lzufesslibrarybook", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.lzufesslibrary.StdLzufe");
		Std.setConfig("inputHdfsPath", newXXXXObjectDir);
		Std.setConfig("outputHdfsPath", stdDir);
		
		JobNode StdNew = new JobNode("lzufesslibrarybook", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.lzufesslibrary.StdLzufe");
		Std.setConfig("inputHdfsPath", rawXXXXObjectDir);
		Std.setConfig("outputHdfsPath", stdDir);

		
		JobNode Temp2Latest = new JobNode("lzufesslibrarybook", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.lzufesslibrary.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);
		
		JobNode Html2Txt = new JobNode("lzufesslibrarybook", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.lzufesslibrary.Html2Txt");
		Html2Txt.setConfig("inputHdfsPath", rawHtmlDir);
		Html2Txt.setConfig("outputHdfsPath", "/user/lqx/output");


		result.add(Html2XXXXObject);
		Html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(Std);
		Std.addChildJob(Temp2Latest);

		
		return result;
	}
}
