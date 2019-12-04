package simple.jobstream.mapreduce.site.pishuinfo;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/pishubook/big_json/2019/20191118";
		String rawXXXXObjectDir = "/RawData/pishubook/XXXXObject";
		String latest_tempDir = "/RawData/pishubook/latest_temp";	//临时成品目录
		String latestDir = "/RawData/pishubook/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/pishubook/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String stdDir = "/RawData/pishubook/new_data/Stdpishuinfo";
		String stdDirA = "/RawData/pishubook/new_data/StdpishuinfoA";

		
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("pishuinfo.Parse",DateTimeHelper.getNowTimeAsBatch(),"simple.jobstream.mapreduce.site.pishuinfo.Json2XXXXObject",
				rawHtmlDir,rawXXXXObjectDir,10);
		
		JobNode MergeXXXXObject2Temp  = JobNodeModel.getJonNode4MergeXXXXObject("pishuinfo.Merge", rawXXXXObjectDir, latestDir, latest_tempDir, 10);
		
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("pishuinfo.Extract", rawXXXXObjectDir, latest_tempDir, newXXXXObjectDir, 10);
		
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("pishuinfo.Copy", latest_tempDir, latestDir);

		
		JobNode StdDb3 = JobNodeModel.getJobNode4Std2Db3("pishuinfo.Std","simple.jobstream.mapreduce.site.pishuinfo.StdPishuInfo", 
				newXXXXObjectDir, stdDir, "pishuinfo","/RawData/_rel_file/zt_template.db3",1);
		
		JobNode StdNew = JobNodeModel.getJobNode4Std2Db3("pishuinfo.Std","simple.jobstream.mapreduce.site.pishuinfo.StdPishuInfo", 
				rawXXXXObjectDir, stdDir, "pishuinfo","/RawData/_rel_file/zt_template.db3",1);
		
		
		JobNode StdALLA = JobNodeModel.getJobNode4Std2Db3("pishuinfo.Std2Db3A","simple.jobstream.mapreduce.common.vip.Std2Db3A4QK", 
				latestDir, stdDirA, "pishuinfo","/RawData/_rel_file/base_obj_meta_a_template_qk.db3",100);
								
		
		JobNode First2Latest = JobNodeModel.getJobNode4CopyXXXXObject("pishuinfo.Copy", rawXXXXObjectDir, latestDir);

		
		//*
		//正常更新
		
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdDb3);
		StdDb3.addChildJob(Temp2Latest);
		
//		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(StdNew);
		
		
		return result;
	}
}
