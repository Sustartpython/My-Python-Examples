package simple.jobstream.mapreduce.site.sinomed_lw;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/SinoMed/boshuo/big_json/2019/20191112";
		String rawXXXXObjectDir = "/RawData/SinoMed/boshuo/XXXXObject";
		String latest_tempDir = "/RawData/SinoMed/boshuo/latest_temp";	//临时成品目录
		String latestDir = "/RawData/SinoMed/boshuo/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/SinoMed/boshuo/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String stdDir2 = "/RawData/SinoMed/boshuo/zt";
		String stdDir = "/RawData/SinoMed/boshuo/a";
		
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("sinomed_lw.Parse", DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.sinomed_lw.Json2XXXXObject", rawHtmlDir, rawXXXXObjectDir, 10);
		
		JobNode StdNew = JobNodeModel.getJobNode4Std2Db3("sinomed_lw.Std","simple.jobstream.mapreduce.site.sinomed_lw.Stdlw", 
				rawXXXXObjectDir, stdDir2, "sinomed_lw","/RawData/_rel_file/zt_template.db3",1);
		
		JobNode First2Latest = JobNodeModel.getJobNode4CopyXXXXObject("sinomed_lw.Copy", rawXXXXObjectDir, latestDir);
		
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("sinomed_lw.Merge", rawXXXXObjectDir, latestDir, latest_tempDir, 10);
		
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("sinomed_lw.Extract", rawXXXXObjectDir, latest_tempDir, newXXXXObjectDir, 10);
		
		JobNode StdDb3 = JobNodeModel.getJobNode4Std2Db3("sinomed_lw.Std","simple.jobstream.mapreduce.site.sinomed_lw.Stdlw", 
				latestDir, stdDir2, "sinomed_lw","/RawData/_rel_file/zt_template.db3",1);
		
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("Stdlw.Copy", latest_tempDir, latestDir);
		
		
		
		
		

		
		//*
		//正常更新
		
//		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
//		MergeXXXXObject2Temp.addChildJob(GenNewData);
//		GenNewData.addChildJob(StdDb3);
//		StdDb3.addChildJob(Temp2Latest);
		
//		第一次上传
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(First2Latest);
		First2Latest.addChildJob(StdDb3);
		
		
		
		return result;
	}
}
