package simple.jobstream.mapreduce.site.academicjournal;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream()
	{
		
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		String rawHtmlDir = "/RawData/academic/big_json/20190718";
		String rawXXXXObjectDir = "/RawData/academic/latest";//新XXXXOBj 输出目录
		String latest_tempDir ="/RawData/academic/latest_temp";//临时存放目录
		String latestDir = "/RawData/academic/XXXXObject";//旧数据存放目录
		String newXXXXObjectDir = "/RawData/academic/new_data";//新数据存放目录
		String stdDir = "/RawData/academic/stdjournal";//DB3目录
		//解析big_json
	    JobNode	XXXXObject = new JobNode("academic", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.academicjournal.Html2XXXXObjectacademic");
	    XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
	    XXXXObject.setConfig("outputHdfsPath", latestDir);
	  //将历史累积数据和新数据合并去重
  		JobNode MergehyXXXXObject2Temp = new JobNode("academic", defaultRootDir,
  				0, "simple.jobstream.mapreduce.site.academicjournal.MergehyXXXXObject2Temp");
  		MergehyXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergehyXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);
  		//生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
  		JobNode GenNewData = new JobNode("academic", defaultRootDir, 
  				0,  "simple.jobstream.mapreduce.site.academicjournal.GenNewData");
  		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);
		//生成DB3
		JobNode Std = new JobNode("academic", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.academicjournal.academicxobj2db");
		Std.setConfig("inputHdfsPath", latestDir);
		Std.setConfig("outputHdfsPath", stdDir);
		
		//备份累积数据
		JobNode  Temp2Latest = new JobNode("academic", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.academicjournal.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);
		//正常更新
		result.add(XXXXObject);
		XXXXObject.addChildJob(MergehyXXXXObject2Temp);
		MergehyXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(Std);
		Std.addChildJob(Temp2Latest);

		return result;
	}
}
