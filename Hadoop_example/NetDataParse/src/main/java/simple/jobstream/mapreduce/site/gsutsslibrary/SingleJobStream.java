package simple.jobstream.mapreduce.site.gsutsslibrary;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		String rawHtmlDir = "/RawData/chaoxing/sslibrary/gsutsslibrarybook/big_json/20190625";
		String rawXXXXObjectDir = "/RawData/chaoxing/sslibrary/gsutsslibrarybook/XXXXObject";
		String latest_tempDir = "/RawData/chaoxing/sslibrary/gsutsslibrarybook/latest_temp";
		String latestDir = "/RawData/chaoxing/sslibrary/gsutsslibrarybook/latest";
		String newXXXXObjectDir = "/RawData/chaoxing/sslibrary/gsutsslibrarybook/new_data/XXXXObject";
		String stdDir = "/RawData/chaoxing/sslibrary/gsutsslibrarybook/new_data/StdDir";

		JobNode Json2XXXXObject = new JobNode("gsutsslibrary", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.gsutsslibrary.Json2XXXXObject");
		Json2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Json2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);

		JobNode MergeXXXXObject2Temp = new JobNode("gsutsslibrary", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.gsutsslibrary.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);

		JobNode GenNewData = new JobNode("gsutsslibrary", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.gsutsslibrary.GenNewData");
		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);

		JobNode StdSslibrary = new JobNode("gsutsslibrary", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.gsutsslibrary.Stdsslibrary");
		StdSslibrary.setConfig("inputHdfsPath", newXXXXObjectDir);
		StdSslibrary.setConfig("outputHdfsPath", stdDir);

		JobNode Temp2Latest = new JobNode("gsutsslibrary", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.gsutsslibrary.Temp2Latest");
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
