package simple.jobstream.mapreduce.site.aiaa_meeting;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/SinoMed/kepu/big_json/2019/20191112";
		String rawXXXXObjectDir = "/RawData/aiaa/hy/XXXXObject";
		String latest_tempDir = "/RawData/aiaa/hy/latest_temp";	//临时成品目录
		String latestDir = "/RawData/aiaa/hy/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/aiaa/hy/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String stdDir = "/RawData/aiaa/hy/new_data/StdAiaaMeeting";
		String stdDir2 = "/RawData/aiaa/hy/new_data/StdAiaaMeeting_zt";
		
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("aiaameeting.Parse", DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.aiaa_meeting.Json2XXXXObject", rawHtmlDir, rawXXXXObjectDir, 10);
		
		JobNode StdNew = JobNodeModel.getJobNode4Std2Db3("aiaameeting.Std","simple.jobstream.mapreduce.site.aiaa_meeting.StdAiaameeting", 
				rawXXXXObjectDir, stdDir2, "aiaa_meeting","/RawData/_rel_file/zt_template.db3",1);
		
		JobNode First2Latest = JobNodeModel.getJobNode4CopyXXXXObject("aiaameeting.Copy", rawXXXXObjectDir, latestDir);
		
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("aiaameeting.Merge", rawXXXXObjectDir, latestDir, latest_tempDir, 10);
		
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("aiaameeting.Extract", rawXXXXObjectDir, latest_tempDir, newXXXXObjectDir, 10);
		
		JobNode StdDb3 = JobNodeModel.getJobNode4Std2Db3("aiaameeting.Std","simple.jobstream.mapreduce.site.aiaa_meeting.StdAiaameeting", 
				newXXXXObjectDir, stdDir2, "aiaa_meeting","/RawData/_rel_file/zt_template.db3",1);
		
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("aiaameeting.Copy", latest_tempDir, latestDir);
		
		JobNode StdTest = JobNodeModel.getJobNode4Std2Db3("aiaameeting.Std","simple.jobstream.mapreduce.common.vip.Std2Db3A", 
				rawXXXXObjectDir, stdDir, "aiaa_meeting","/RawData/_rel_file/base_obj_meta_a_template_hy.db3",1);
		
		JobNode Db32XXXXObject = JobNodeModel.getJobNode4Sqlite2XXXXObject("bioone.xxobj", "base_obj_meta_a", "lngid", stdDir, newXXXXObjectDir);
		
		// 更新a层
		JobNode Latest2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("aiaameeting.Parse", DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.aiaa_meeting.StdAiaameeting_2015", latestDir, newXXXXObjectDir, 10);
		JobNode Stdall = JobNodeModel.getJobNode4Std2Db3("ascejournal.Std","simple.jobstream.mapreduce.common.vip.Std2Db3A", 
				newXXXXObjectDir, stdDir, "aiaameeing","/RawData/_rel_file/base_obj_meta_a_template_hy.db3",1);
		
		//*
		//正常更新
		
//		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
//		MergeXXXXObject2Temp.addChildJob(GenNewData);
//		GenNewData.addChildJob(StdDb3);
//		StdDb3.addChildJob(Temp2Latest);
		
		result.add(Latest2XXXXObject);
		Latest2XXXXObject.addChildJob(Stdall);
		
		
		return result;
	}
}
