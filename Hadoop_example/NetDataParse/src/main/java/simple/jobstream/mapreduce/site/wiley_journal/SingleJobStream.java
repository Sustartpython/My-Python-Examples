package simple.jobstream.mapreduce.site.wiley_journal;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/wiley/journal/big_json/2018/20181024";
		String rawXXXXObjectDir = "/RawData/wiley/journal/XXXXObject";//新XXXXOBj 输出目录
		String latest_tempDir ="/RawData/wiley/journal/latest_temp";//临时存放目录
		String latestDir = "/RawData/wiley/journal/latest";//旧数据存放目录
		String newXXXXObjectDir = "/RawData/wiley/journal/new_data";//新数据存放目录
		String stdhyDir = "/RawData/wiley/journal/stdjournal";//DB3目录
		//解析big_json
	    JobNode	XXXXObjectwiley = new JobNode("wileyjournal", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.wiley_journal.Html2XXXXObjectwiley");
	    XXXXObjectwiley.setConfig("inputHdfsPath", rawHtmlDir);
	    XXXXObjectwiley.setConfig("outputHdfsPath", rawXXXXObjectDir);
	  //将历史累积数据和新数据合并去重
  		JobNode MergehyXXXXObject2Temp = new JobNode("wileyjournal", defaultRootDir,
  				0, "simple.jobstream.mapreduce.site.wiley_journal.MergehyXXXXObject2Temp");
  		MergehyXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergehyXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);
  		//生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
  		JobNode GenNewData = new JobNode("wileyjournal", defaultRootDir, 
  				0,  "simple.jobstream.mapreduce.site.wiley_journal.GenNewData");
  		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);
		//生成DB3
		JobNode Stdwilye = new JobNode("wileyjournal", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.wiley_journal.wiley_journal_db");
		Stdwilye.setConfig("inputHdfsPath", newXXXXObjectDir);
		Stdwilye.setConfig("outputHdfsPath", stdhyDir);
		
		//备份累积数据
		JobNode  Temp2Latest = new JobNode("wileyjournal", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.wiley_journal.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);
		//正常更新
		result.add(XXXXObjectwiley);
		XXXXObjectwiley.addChildJob(MergehyXXXXObject2Temp);
		MergehyXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(Stdwilye);
		Stdwilye.addChildJob(Temp2Latest);

		return result;
	}
}
