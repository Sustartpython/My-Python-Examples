package simple.jobstream.mapreduce.site.chaoxingjournal;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/chaoxing/chaoxingjournal/big_json/2019/20190626";
		String rawXXXXObjectDir = "/RawData/chaoxing/chaoxingjournal/XXXXObject";
		String latest_tempDir = "/RawData/chaoxing/chaoxingjournal/latest_temp";	//临时成品目录
		String latestDir = "/RawData/chaoxing/chaoxingjournal/latest";	//成品目录
		String latestMetaDir = "/RawData/chaoxing/chaoxingjournal/latest_meta";	//成品目录
		String newXXXXObjectDir = "/RawData/chaoxing/chaoxingjournal/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String stdDir = "/RawData/chaoxing/chaoxingjournal/new_data/Stdchaoxingjournal";
		String stdDirA = "/RawData/chaoxing/chaoxingjournal/new_data/StdchaoxingjournalA";

//		JobNode Json2XXXXObject = new JobNode("chaoxingjournal", defaultRootDir,
//				0, "simple.jobstream.mapreduce.site.chaoxingjournal.Json2XXXXObject");		
//		Json2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
//		Json2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);
		
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("chaoxingjournal.Parse",DateTimeHelper.getNowTimeAsBatch(),"simple.jobstream.mapreduce.site.chaoxingjournal.Json2XXXXObject",
				rawHtmlDir,rawXXXXObjectDir,100);
		
		JobNode JsonTxt2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("chaoxingjournal.Parse",DateTimeHelper.getNowTimeAsBatch(),"simple.jobstream.mapreduce.site.chaoxingjournal.JsonTxt2XXXXObject",
				rawHtmlDir,rawXXXXObjectDir,100);
		
		JobNode MergeXXXXObject2Temp  = JobNodeModel.getJonNode4MergeXXXXObject("chaoxingjournal.Merge", rawXXXXObjectDir, latestDir, latest_tempDir, 100);
		
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("chaoxingjournal.Extract", rawXXXXObjectDir, latest_tempDir, newXXXXObjectDir, 100);
		
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("chaoxingjournal.Copy", latest_tempDir, latestDir);

		
		JobNode StdDb3 = JobNodeModel.getJobNode4Std2Db3("chaoxingjournal.Std","simple.jobstream.mapreduce.site.chaoxingjournal.StdChaoxing2", 
				newXXXXObjectDir, stdDir, "chaoxingjournal","/RawData/_rel_file/zt_template.db3",5);
		
		JobNode StdTxt = JobNodeModel.getJobNode4Std2Db3("chaoxingjournal.Std","simple.jobstream.mapreduce.site.chaoxingjournal.StdChaoxingTxt", 
				latestDir, stdDir, "chaoxingjournal","/user/lqx/chaoxingjournal/zt_template.db3",20);
		
		JobNode StdNew = JobNodeModel.getJobNode4Std2Db3("chaoxingjournal.Std","simple.jobstream.mapreduce.site.chaoxingjournal.StdChaoxing2", 
				rawXXXXObjectDir, stdDir, "chaoxingjournal","/RawData/_rel_file/zt_template.db3",5);
		
		JobNode StdDel = JobNodeModel.getJobNode4Std2Db3("chaoxingjournal.Std","simple.jobstream.mapreduce.site.chaoxingjournal.StdChaoxingDelete", 
				latestDir, stdDir, "chaoxingjournal","/RawData/_rel_file/zt_template.db3",1);
		
		JobNode StdALL = JobNodeModel.getJobNode4Std2Db3("chaoxingjournal.Std","simple.jobstream.mapreduce.site.chaoxingjournal.StdChaoxing", 
				latest_tempDir, stdDir, "chaoxingjournal","/RawData/_rel_file/zt_template.db3",100);
		
		JobNode StdALLA = JobNodeModel.getJobNode4Std2Db3("chaoxingjournal.Std2Db3A","simple.jobstream.mapreduce.common.vip.Std2Db3A", 
				newXXXXObjectDir, stdDirA, "chaoxingjournal","/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3",5);
								
		
		JobNode First2Latest = JobNodeModel.getJobNode4CopyXXXXObject("chaoxingjournal.Copy", rawXXXXObjectDir, latestDir);
		
		JobNode Change2A = new JobNode("Change2AChaoxing", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.chaoxingjournal.Change2AChaoxing");
		Change2A.setConfig("inputHdfsPath", latestDir);
		Change2A.setConfig("outputHdfsPath", latestMetaDir);
		Change2A.setConfig("reduceNum", String.valueOf(200));
		
		String testInput = "/user/chenrui/mapinfo";
		String hfileDir	= "/user/lqx/hfileDir";
		
		// 将 XXXXObject 转为 HFile 文件
		JobNode XXXXObject2HFile = JobNodeModel.getJobNode4XXXXObject2HFile("base_obj_meta_a.XXXXObject2HFile",
				testInput, hfileDir, "lqx_test_obj_meta_a", "main", 400, true);

		// 加载 HFile 到 HBase
		JobNode LoadHFile = JobNodeModel.getJobNode4LoadHFile("base_obj_meta_a.LoadHFile", hfileDir, "lqx_test_obj_meta_a",
				"main");

		
		//*
		//正常更新
		
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdDb3);
		StdDb3.addChildJob(Temp2Latest);
		
//		result.add(XXXXObject2HFile);
//		XXXXObject2HFile.addChildJob(LoadHFile);
		
		
		return result;
	}
}
