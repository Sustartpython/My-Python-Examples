package simple.jobstream.mapreduce.site.sinomed_kp;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/SinoMed/kepu/big_json/2019/20191112";
		String rawXXXXObjectDir = "/RawData/SinoMed/kepu/XXXXObject";
		String latest_tempDir = "/RawData/SinoMed/kepu/latest_temp";	//临时成品目录
		String latestDir = "/RawData/SinoMed/kepu/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/SinoMed/kepu/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String stdDir2 = "/RawData/SinoMed/kepu/zt";
		String stdDir = "/RawData/SinoMed/kepu/a";
		
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("sinomed_kp.Parse", DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.sinomed_kp.Json2XXXXObject", rawHtmlDir, rawXXXXObjectDir, 10);
		
		JobNode StdNew = JobNodeModel.getJobNode4Std2Db3("sinomed_kp.Std","simple.jobstream.mapreduce.site.sinomed_kp.Stdkp", 
				rawXXXXObjectDir, stdDir2, "sinomed_kp","/RawData/_rel_file/zt_template.db3",1);
		
		JobNode First2Latest = JobNodeModel.getJobNode4CopyXXXXObject("sinomed_kp.Copy", rawXXXXObjectDir, latestDir);
		
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("sinomed_kp.Merge", rawXXXXObjectDir, latestDir, latest_tempDir, 10);
		
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("sinomed_kp.Extract", rawXXXXObjectDir, latest_tempDir, newXXXXObjectDir, 10);
		
		JobNode StdDb3 = JobNodeModel.getJobNode4Std2Db3("sinomed_kp.Std","simple.jobstream.mapreduce.site.sinomed_kp.Stdkp", 
				newXXXXObjectDir, stdDir2, "sinomed_kp","/RawData/_rel_file/zt_template.db3",1);
		
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("sinomed_kp.Copy", latest_tempDir, latestDir);
		
		
		
		
		

		
		//*
		//正常更新
		
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdDb3);
		StdDb3.addChildJob(Temp2Latest);
		
//		第一次上传
//		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(First2Latest);
//		First2Latest.addChildJob(StdDb3);
		
		
		
		return result;
	}
}
