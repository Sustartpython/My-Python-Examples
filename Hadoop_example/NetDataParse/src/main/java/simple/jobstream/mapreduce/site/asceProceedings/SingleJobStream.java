package simple.jobstream.mapreduce.site.asceProceedings;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;
import simple.jobstream.mapreduce.site.jstor_qk.jstorJson2XXXXObject2;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/asce/asceproceedings/big_json/2019/20191111";
		String rawXXXXObjectDir = "/RawData/asce/asceproceedings/XXXXObject";
		String latest_tempDir = "/RawData/asce/asceproceedings/latest_temp";	//临时成品目录
		String latestDir = "/RawData/asce/asceproceedings/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/asce/asceproceedings/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String stdDir = "/RawData/asce/asceproceedings/new_data/StdAsceproceedings";
		String stdDir2 = "/RawData/asce/asceproceedings/new_data/StdAsceproceedings_zt";
		
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("asceProceedings.Parse", DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.asceProceedings.Json2XXXXObject", rawHtmlDir, rawXXXXObjectDir, 10);
		
		JobNode StdNew = JobNodeModel.getJobNode4Std2Db3("asceProceedings.Std","simple.jobstream.mapreduce.site.asceProceedings.StdAsceproceedings", 
				rawXXXXObjectDir, stdDir2, "asceProceedings","/RawData/_rel_file/zt_template.db3",1);
		
		JobNode First2Latest = JobNodeModel.getJobNode4CopyXXXXObject("asceProceedings.Copy", rawXXXXObjectDir, latestDir);
		
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("asceProceedings.Merge", rawXXXXObjectDir, latestDir, latest_tempDir, 10);
		
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("asceProceedings.Extract", rawXXXXObjectDir, latest_tempDir, newXXXXObjectDir, 10);
		
		JobNode StdDb3 = JobNodeModel.getJobNode4Std2Db3("asceProceedings.Std","simple.jobstream.mapreduce.site.asceProceedings.StdAsceproceedings", 
				newXXXXObjectDir, stdDir2, "asceProceedings","/RawData/_rel_file/zt_template.db3",1);
		
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("asceProceedings.Copy", latest_tempDir, latestDir);
		
		JobNode copy1 = JobNodeModel.getJobNode4CopyXXXXObject("asceProceedings.Copy", rawXXXXObjectDir, latestDir);
		
		JobNode StdTest = JobNodeModel.getJobNode4Std2Db3("asceProceedings.Std","simple.jobstream.mapreduce.common.vip.Std2Db3A", 
				rawXXXXObjectDir, stdDir, "asceProceedings","/RawData/_rel_file/base_obj_meta_a_template_hy.db3",1);
		
		JobNode Db32XXXXObject = JobNodeModel.getJobNode4Sqlite2XXXXObject("asceProceedings.xxobj", "base_obj_meta_a", "lngid", stdDir, newXXXXObjectDir);
		
		
		// 更新a层(大于2015)
		JobNode Latest2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("asceconference.Parse", DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.asceProceedings.Std_2015", latestDir, newXXXXObjectDir, 10);
		JobNode Stdall = JobNodeModel.getJobNode4Std2Db3("asceconference.Std","simple.jobstream.mapreduce.common.vip.Std2Db3A", 
						newXXXXObjectDir, stdDir, "asceconference","/RawData/_rel_file/base_obj_meta_a_template_hy.db3",1);
				
		
		//*
		//正常更新
		
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdDb3);
		StdTest.addChildJob(Temp2Latest);
//		
//		result.add(Latest2XXXXObject);
//		Latest2XXXXObject.addChildJob(Stdall);
		
		return result;
	}
}
