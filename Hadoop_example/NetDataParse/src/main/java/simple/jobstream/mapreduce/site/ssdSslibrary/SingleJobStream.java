package simple.jobstream.mapreduce.site.ssdSslibrary;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";

		String rawHtmlDir = "/RawData/chaoxing/sslibrary/snnusslibrarybook/big_json/2019/20190619";
		String rawXXXXObjectDir = "/RawData/chaoxing/sslibrary/snnusslibrarybook/XXXXObject";
		String latest_tempDir = "/RawData/chaoxing/sslibrary/snnusslibrarybook/latest_temp";
		String latestDir = "/RawData/chaoxing/sslibrary/snnusslibrarybook/latest";
		String newXXXXObjectDir = "/RawData/chaoxing/sslibrary/snnusslibrarybook/new_data/XXXXObject";
		String stdDir = "/RawData/chaoxing/sslibrary/snnusslibrarybook/new_data/StdDir";

		JobNode Json2XXXXObject = new JobNode("SslibraryParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.ssdSslibrary.Json2XXXXObject");
		Json2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Json2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);

		
		JobNode MergeXXXXObject2Temp = new JobNode("ssdlibraryParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.ssdSslibrary.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);


		JobNode GenNewData = new JobNode("SslibraryParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.ssdSslibrary.GenNewData");
		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);

		JobNode StdSslibrary = new JobNode("SslibraryParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.ssdSslibrary.StdSslibrary");
		StdSslibrary.setConfig("inputHdfsPath", newXXXXObjectDir);
		StdSslibrary.setConfig("outputHdfsPath", stdDir);


		JobNode Temp2Latest = new JobNode("SslibraryParse", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.ssdSslibrary.Temp2Latest");
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
