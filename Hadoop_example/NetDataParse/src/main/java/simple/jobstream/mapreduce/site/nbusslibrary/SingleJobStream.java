package simple.jobstream.mapreduce.site.nbusslibrary;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		String rawHtmlDir = "/RawData/chaoxing/nbusslibrarybook/big_json/20190114";
		String rawXXXXObjectDir = "/RawData/chaoxing/nbusslibrarybook/XXXXObject";
		String latest_tempDir = "/RawData/chaoxing/nbusslibrarybook/latest_temp";
		String latestDir = "/RawData/chaoxing/nbusslibrarybook/latest";
		String newXXXXObjectDir = "/RawData/chaoxing/nbusslibrarybook/new_data/XXXXObject";
		String stdDir = "/RawData/chaoxing/nbusslibrarybook/new_data/StdDir";

		JobNode Json2XXXXObject = new JobNode("SslibraryParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.nbusslibrary.Json2XXXXObject");
		Json2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Json2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);

		
		JobNode MergeXXXXObject2Temp = new JobNode("SslibraryParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.nbusslibrary.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);

		
		JobNode GenNewData = new JobNode("SslibraryParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.nbusslibrary.GenNewData");
		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);

		JobNode StdSslibrary = new JobNode("SslibraryParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.nbusslibrary.Stdsslibrary");
		StdSslibrary.setConfig("inputHdfsPath", newXXXXObjectDir);
		StdSslibrary.setConfig("outputHdfsPath", stdDir);

		
		JobNode Temp2Latest = new JobNode("SslibraryParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.nbusslibrary.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);

		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdSslibrary);
		StdSslibrary.addChildJob(Temp2Latest);
		return result;
	}
}
