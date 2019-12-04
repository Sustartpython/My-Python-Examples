package simple.jobstream.mapreduce.site.bioonejournal;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/bioonejournal/big_json/2019/20191202";
		String rawXXXXObjectDir = "/RawData/bioonejournal/XXXXObject";
		String latest_tempDir = "/RawData/bioonejournal/latest_temp";	//临时成品目录
		String latestDir = "/RawData/bioonejournal/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/bioonejournal/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String stdDir = "/RawData/bioonejournal/new_data/StdBio";
		
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("bioone.Parse", DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.bioonejournal.Json2XXXXObject", rawHtmlDir, rawXXXXObjectDir, 10);
		
		JobNode StdNew = JobNodeModel.getJobNode4Std2Db3("bioone.Std","simple.jobstream.mapreduce.site.bioonejournal.StdBioone", 
				rawXXXXObjectDir, stdDir, "bioonejournal","/RawData/_rel_file/zt_template.db3",1);
		
		JobNode First2Latest = JobNodeModel.getJobNode4CopyXXXXObject("bioone.Copy", rawXXXXObjectDir, latestDir);
		
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("bioone.Merge", rawXXXXObjectDir, latestDir, latest_tempDir, 10);
		
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("bioone.Extract", rawXXXXObjectDir, latest_tempDir, newXXXXObjectDir, 10);
		
		JobNode StdDb3 = JobNodeModel.getJobNode4Std2Db3("bioone.Std","simple.jobstream.mapreduce.site.bioonejournal.StdBioone", 
				newXXXXObjectDir, stdDir, "bioonejournal","/RawData/_rel_file/zt_template.db3",1);
		
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("bioone.Copy", latest_tempDir, latestDir);
		
		JobNode StdTest = JobNodeModel.getJobNode4Std2Db3("bioone.Std","simple.jobstream.mapreduce.common.vip.Std2Db3A", 
				latestDir, stdDir, "bioonejournal","/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3",3);
		
		JobNode Db32XXXXObject = JobNodeModel.getJobNode4Sqlite2XXXXObject("bioone.xxobj", "base_obj_meta_a", "lngid", stdDir, newXXXXObjectDir);
		
		
		//*
		//正常更新
		
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdDb3);
		StdDb3.addChildJob(Temp2Latest);
		
		
		
		return result;
	}
}
