package simple.jobstream.mapreduce.site.sslibrary;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";///RawData/chaoxing/sslibrary/sslibrarybook/big_json/20190613
		String rawHtmlDir = "/RawData/chaoxing/sslibrary/sslibrarybook/big_json/20190714";
		String rawXXXXObjectDir = "/RawData/chaoxing/sslibrary/sslibrarybook/XXXXObject";
		String latest_tempDir = "/RawData/chaoxing/sslibrary/sslibrarybook/latest_temp";
		String latestDir = "/RawData/chaoxing/sslibrary/sslibrarybook/latest";
		String newXXXXObjectDir = "RawData/chaoxing/sslibrary/sslibrarybook/new_data/XXXXObject";
		String stdDir = "/RawData/chaoxing/sslibrary/sslibrarybook/new_data/StdDir";

		JobNode Json2XXXXObject = new JobNode("sslibrary", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.sslibrary.Json2XXXXObject");
		Json2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Json2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);

		JobNode MergeXXXXObject2Temp = new JobNode("sslibrary", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.sslibrary.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);

		JobNode GenNewData = new JobNode("sslibrary", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.sslibrary.GenNewData");
		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);

		JobNode StdSslibrary = new JobNode("sslibrary", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.sslibrary.StdSslibrary");
		StdSslibrary.setConfig("inputHdfsPath", newXXXXObjectDir);
		StdSslibrary.setConfig("outputHdfsPath", stdDir);

		JobNode Temp2Latest = new JobNode("sslibrary", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.sslibrary.Temp2Latest");
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
