package simple.jobstream.mapreduce.site.duxiuBook;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";

		String rawHtmlDir = "/RawData/chaoxing/duxiu_ts/big_htm/20190627";
		String rawXXXXObjectDir = "/RawData/chaoxing/duxiu_ts/XXXXObject";
		String latest_tempDir = "/RawData/chaoxing/duxiu_ts/latest_temp";
		String latestDir = "/RawData/chaoxing/duxiu_ts/latest";
		String latestback = "/RawData/chaoxing/duxiu_ts/latest_a_back";
		String newXXXXObjectDir = "/RawData/chaoxing/duxiu_ts/new_data/XXXXObject";
		String stdDir = "/RawData/chaoxing/duxiu_ts/new_data/StdDir";
		
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("Duxiu.Parse", DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.duxiuBook.Json2XXXXObject", rawHtmlDir, rawXXXXObjectDir, 10);
		
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("Duxiu.Merge", rawXXXXObjectDir, latestDir, 
				latest_tempDir, 20);
		
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("Duxiu.Extract", rawXXXXObjectDir, latest_tempDir,
				newXXXXObjectDir, 10);
		
		JobNode StdDuxiu = JobNodeModel.getJobNode4Std2Db3("Duxiu.Std","simple.jobstream.mapreduce.site.duxiuBook.StdDuxiu", 
				newXXXXObjectDir, stdDir, "duxiubook","/RawData/chaoxing/duxiu_ts/ref_file/duxiu_ts_template.db3",1);
		
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("Duxiu.Copy", latest_tempDir, latestDir);

		
		JobNode StdDuxiuA = JobNodeModel.getJobNode4Std2Db3("duxiu.Std2Db3A","simple.jobstream.mapreduce.common.vip.Std2Db3A", 
				newXXXXObjectDir, stdDir, "duxiu_meta","/RawData/_rel_file/base_obj_meta_a_template_ts.db3",1);
		
		JobNode StdNew = JobNodeModel.getJobNode4Std2Db3("Duxiu.Std","simple.jobstream.mapreduce.site.duxiuBook.StdDuxiu", 
				latestDir, stdDir, "duxiubook","/RawData/chaoxing/duxiu_ts/ref_file/duxiu_ts_template.db3",10);

		result.add(Json2XXXXObject);	
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdDuxiu);
		StdDuxiu.addChildJob(Temp2Latest);
		
//		JobNode ChangeLngid = new JobNode("Duxiu.ChangeLngid", defaultRootDir, 0, 
//				"simple.jobstream.mapreduce.site.duxiuBook.ChangeLngid");
//		ChangeLngid.setConfig("inputHdfsPath", latestback);
//		ChangeLngid.setConfig("outputHdfsPath", latestDir);
//		ChangeLngid.setConfig("reduceNum", String.valueOf(20));
//		
//		JobNode StdTxt = new JobNode("Duxiu.txt", defaultRootDir, 0, 
//				"simple.jobstream.mapreduce.site.duxiuBook.Std2TXT");
//		StdTxt.setConfig("inputHdfsPath", latestDir);
//		StdTxt.setConfig("outputHdfsPath", "/user/lqx/duxiu");
//		
//		JobNode Txt2ZLF = new JobNode("Duxiu.txt", defaultRootDir, 0, 
//				"simple.jobstream.mapreduce.site.duxiuBook.TXT2ZLF");
//		Txt2ZLF.setConfig("inputHdfsPath", "/user/lqx/book/zlf_ts_7_1");
//		Txt2ZLF.setConfig("outputHdfsPath", "/user/lqx/duxiuzlf");
//
//		
//		
//
//		result.add(Txt2ZLF);
//		ChangeLngid.addChildJob(StdNew);
//		StdNew.addChildJob(StdTxt);
		return result;
	}
}
