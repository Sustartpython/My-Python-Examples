package simple.jobstream.mapreduce.site.apajournal;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/apajournal/big_json/20190416";
		String rawXXXXObjectDir = "/RawData/apajournal/XXXXObject";
		String latest_tempDir = "/RawData/apajournal/latest_temp";	//临时成品目录
		String latestDir = "/RawData/apajournal/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/apajournal/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String stdDir = "/RawData/apajournal/new_data/StdBio";
		
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("apa.Parse", DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.apajournal.Json2XXXXObject", rawHtmlDir, rawXXXXObjectDir, 10);
		
		JobNode StdNew = JobNodeModel.getJobNode4Std2Db3("apa.Std","simple.jobstream.mapreduce.site.apajournal.Stdapa", 
				rawXXXXObjectDir, stdDir, "apajournal","/RawData/_rel_file/zt_template.db3",1);
		
		JobNode First2Latest = JobNodeModel.getJobNode4CopyXXXXObject("apa.Copy", rawXXXXObjectDir, latestDir);
		
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("apa.Merge", rawXXXXObjectDir, latestDir, latest_tempDir, 10);
		
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("apa.Extract", rawXXXXObjectDir, latest_tempDir, newXXXXObjectDir, 10);
		
		JobNode StdDb3 = JobNodeModel.getJobNode4Std2Db3("apa.Std","simple.jobstream.mapreduce.site.apajournal.Stdapa", 
				newXXXXObjectDir, stdDir, "apajournal","/RawData/_rel_file/zt_template.db3",1);
		
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("apa.Copy", latest_tempDir, latestDir);
		
		//*
		//正常更新
		
		result.add(Json2XXXXObject);
//		First2Latest.addChildJob(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdDb3);
		StdDb3.addChildJob(Temp2Latest);
		
		
		
		return result;
	}
}
