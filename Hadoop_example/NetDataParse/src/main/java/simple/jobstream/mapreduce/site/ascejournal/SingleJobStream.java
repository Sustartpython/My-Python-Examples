package simple.jobstream.mapreduce.site.ascejournal;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/asce/ascejournal/big_json/2019/20191202";
		String rawXXXXObjectDir = "/RawData/asce/ascejournal/XXXXObject";
		String latest_tempDir = "/RawData/asce/ascejournal/latest_temp";	//临时成品目录
		String latestDir = "/RawData/asce/ascejournal/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/asce/ascejournal/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String stdDir = "/RawData/asce/ascejournal/new_data/Stdascejournal";
		String stdDir2 = "/RawData/asce/ascejournal/new_data/Stdascejournal_zt";
		
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("ascejournal.Parse", DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.ascejournal.Json2XXXXObject", rawHtmlDir, rawXXXXObjectDir, 10);
		
		JobNode Latest2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("ascejournal.Parse", DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.ascejournal.StdAscejournal_2015", latestDir, newXXXXObjectDir, 10);
		
		JobNode StdNew = JobNodeModel.getJobNode4Std2Db3("ascejournal.Std","simple.jobstream.mapreduce.site.ascejournal.StdAscejournal", 
				rawXXXXObjectDir, stdDir2, "ascejournal","/RawData/_rel_file/zt_template.db3",1);
		
		JobNode First2Latest = JobNodeModel.getJobNode4CopyXXXXObject("ascejournal.Copy", rawXXXXObjectDir, latestDir);
		
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("ascejournal.Merge", rawXXXXObjectDir, latestDir, latest_tempDir, 10);
		
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("ascejournal.Extract", rawXXXXObjectDir, latest_tempDir, newXXXXObjectDir, 10);
		
		JobNode StdDb3 = JobNodeModel.getJobNode4Std2Db3("ascejournal.Std","simple.jobstream.mapreduce.site.ascejournal.StdAscejournal", 
				newXXXXObjectDir, stdDir2, "ascejournal","/RawData/_rel_file/zt_template.db3",1);
		
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("ascejournal.Copy", latest_tempDir, latestDir);
		
		
		JobNode Stdall = JobNodeModel.getJobNode4Std2Db3("ascejournal.Std","simple.jobstream.mapreduce.common.vip.Std2Db3A", 
				newXXXXObjectDir, stdDir, "ascejournal","/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3",1);
		
		JobNode Db32XXXXObject = JobNodeModel.getJobNode4Sqlite2XXXXObject("ascejournal.xxobj", "base_obj_meta_a", "lngid", stdDir, newXXXXObjectDir);
		
		
		//*
		//正常更新
		
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdDb3);
		StdDb3.addChildJob(Temp2Latest);
		
		
//		result.add(Latest2XXXXObject);
//		Latest2XXXXObject.addChildJob(Stdall);
		
//		result.add(StdTest);
		
		
		
		return result;
	}
}
