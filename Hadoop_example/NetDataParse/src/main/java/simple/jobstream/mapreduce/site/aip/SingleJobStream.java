package simple.jobstream.mapreduce.site.aip;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";

		String rawHtmlDir = "/RawData/aipjournal/big_json/2019/20191202";
		String rawXXXXObjectDir = "/RawData/aipjournal/XXXXObject";
		String latest_tempDir = "/RawData/aipjournal/latest_temp"; 
		String latestDir = "/RawData/aipjournal/latest"; 
		String newXXXXObjectDir = "/RawData/aipjournal/new_data/XXXXObject";
		String stdDir = "/RawData/aipjournal/new_data/StdDir";


		JobNode Html2XXXXObject = new JobNode("Aipjournal", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.aip.Html2XXXXObject");
		Html2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Html2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);

		
		JobNode MergeXXXXObject2Temp = new JobNode("Aipjournal", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.aip.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);

		
		JobNode GenNewData = new JobNode("Aipjournal", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.aip.GenNewData");
		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);

		JobNode Std = new JobNode("Aipjournal", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.aip.StdAIP");
		Std.setConfig("inputHdfsPath", newXXXXObjectDir);
		Std.setConfig("outputHdfsPath", stdDir);

		
		JobNode Temp2Latest = new JobNode("Aipjournal", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.aip.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);
		
		JobNode Stdtest = new JobNode("Aipjournal", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.aip.StdAIP");
		Stdtest.setConfig("inputHdfsPath", rawXXXXObjectDir);
		Stdtest.setConfig("outputHdfsPath", stdDir);
		
		JobNode FindHtml = new JobNode("Aipjournal", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.aip.FindHtml");
		FindHtml.setConfig("inputHdfsPath", "/RawData/aipjournal/big_json/2018/20180711");
		FindHtml.setConfig("outputHdfsPath", "/user/site/duxiu");
		
		// 取出title为空的创建db3
		JobNode StdFirst = new JobNode("Aipjournal", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.aip.StdAIP");
		StdFirst.setConfig("inputHdfsPath", latestDir);
		StdFirst.setConfig("outputHdfsPath", stdDir);
		
		JobNode First2Latest = new JobNode("Aipjournal", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.aip.Temp2Latest");
		First2Latest.setConfig("inputHdfsPath", rawXXXXObjectDir);
		First2Latest.setConfig("outputHdfsPath", latestDir);
		
		JobNode CountHY = new JobNode("Aipjournal", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.aip.CountHY");
		CountHY.setConfig("inputHdfsPath", latestDir);
		CountHY.setConfig("outputHdfsPath", "/user/lqx/duxiu");

		
		JobNode Std2015 = new JobNode("Aipjournal", defaultRootDir, 0,
				"simple.jobstream.mapreduce.site.aip.StdAIP_2015");
		Std2015.setConfig("inputHdfsPath", latestDir);
		Std2015.setConfig("outputHdfsPath", newXXXXObjectDir);
		Std2015.setConfig("reduceNum", "5");
		
		JobNode Stdall = JobNodeModel.getJobNode4Std2Db3("aipjournal.Std","simple.jobstream.mapreduce.common.vip.Std2Db3A", 
				newXXXXObjectDir, stdDir, "aipjournal","/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3",1);
		
		
		result.add(Html2XXXXObject);
		Html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(Std);
		Std.addChildJob(Temp2Latest);
		
//		result.add(StdFirst);
//		result.add(Std2015);
//		Std2015.addChildJob(Stdall);

		
		
		
		
		return result;
	}
}
