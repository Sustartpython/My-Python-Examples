package simple.jobstream.mapreduce.site.aps;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/apsjournal/big_json/2019/20191202";
		String rawXXXXObjectDir = "/RawData/apsjournal/XXXXObject";
		String latest_tempDir = "/RawData/apsjournal/latest_temp";	//临时成品目录
		String latestDir = "/RawData/apsjournal/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/apsjournal/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String stdDirA = "/RawData/apsjournal/new_data/StdAPS";
		String stdDirzt = "/RawData/apsjournal/new_data/StdAPSzt";
		
		
		
		JobNode Json2XXXXObject = new JobNode("APSJournal", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.aps.Json2XXXXObject");		
		Json2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Json2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);
		
		//将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp  = new JobNode("APSJournal", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.aps.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);
		
		//生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = new JobNode("APSJournal", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.aps.GenNewData");
		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);
		
		JobNode StdDb3 = new JobNode("APSJournal", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.aps.StdAPS");	
		StdDb3.setConfig("inputHdfsPath", newXXXXObjectDir);
		StdDb3.setConfig("outputHdfsPath", stdDirzt);
		
		
		
		//备份累积数据
		JobNode Temp2Latest = new JobNode("APSJournal", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.aps.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);
		
		
		JobNode StdNew = new JobNode("APSJournal", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.aps.StdAPS2");	
		StdNew.setConfig("inputHdfsPath", latestDir);
		StdNew.setConfig("outputHdfsPath", stdDirzt);
		
		
		JobNode Json2Txt = new JobNode("APSJournal", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.aps.Json2Txt");		
		Json2Txt.setConfig("inputHdfsPath", rawHtmlDir);
		Json2Txt.setConfig("outputHdfsPath", "/user/lqx/test");
		
		
		JobNode First2Latest = new JobNode("APSJournal", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.aps.Temp2Latest");
		First2Latest.setConfig("inputHdfsPath", rawXXXXObjectDir);
		First2Latest.setConfig("outputHdfsPath", latestDir);
		
		JobNode ChangeALatest = new JobNode("APSJournal", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.aps.Change2ALatest");
		ChangeALatest.setConfig("inputHdfsPath", latestDir);
		ChangeALatest.setConfig("outputHdfsPath", rawXXXXObjectDir);
		
		JobNode StdTest = JobNodeModel.getJobNode4Std2Db3("aps.Std","simple.jobstream.mapreduce.common.vip.Std2Db3A", 
				rawXXXXObjectDir, stdDirA, "aps","/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3",1);
//		
		
		
		//*
		//正常更新
		
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdDb3);
		StdDb3.addChildJob(Temp2Latest);
		
//		result.add(ChangeALatest);
//		ChangeALatest.addChildJob(StdTest);
		
		
		
//		result.add(StdNew);
//		Json2XXXXObject.addChildJob(StdNew);
		
		/*/
		
		//导出数据到db3
		StdDb3.setConfig("inputHdfsPath", latestDir);
		StdDb3.setConfig("outputHdfsPath", stdDir);
		result.add(StdDb3);
		//*/
		

		
		return result;
	}
}
