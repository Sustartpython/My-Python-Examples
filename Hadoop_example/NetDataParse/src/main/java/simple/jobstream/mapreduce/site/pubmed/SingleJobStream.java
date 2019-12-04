package simple.jobstream.mapreduce.site.pubmed;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String rootDir = "";
		String defaultRootDir="";

		// // 带解析新数据路径test"
		String rawDataDir = "/RawData/pubmed/big_json/2019/20190716";
		String rawDataXXXXObjectDir = "/RawData/pubmed/xxxxobject"; // 新数据解析后的XXXXObject格式数据路径
//		String rawDataXXXXObjectDir ="/RawData/pubmed/xxxtest";   // 临时测试使用
		String latest_tempDir = "/RawData/pubmed/latest_temp"; // 新数据和旧数据合并去重得到临时数据的存放路径
		String latestDir = "/RawData/pubmed/latest"; // 将latest_tempDir数据保存到该目录，以备下次更新使用

		String new_data_xxxxobject = "/RawData/pubmed/new_data/xxxxobject"; // 去重后得到的新数据
		String new_data_stdDir = "/RawData/pubmed/new_data/stdfile"; // 新数据转换为DB3格式存放目录
		String TeststdDir = "/user/qianjun/output/pubmed";
		String CheckstdDir = "/user/qianjun/output/pubmedtest";
		String  new_data_stdDir_zhitu="/RawData/pubmed/zhituDate/stdfile";
		

		// 第一步：将最近增加的big_htm文件转换为xxxxobjcet文件存放在xxxxobject目录下
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("Pubmed.Json2XXXXObject",
				DateTimeHelper.getNowTimeAsBatch(), "simple.jobstream.mapreduce.site.pubmed.Json2XXXXObjectNew",
				rawDataDir, rawDataXXXXObjectDir, 200);

		// 将历史累积数据和新数据合并去重

		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("Pubmed.MergeXXXXObject2Temp",
				rawDataXXXXObjectDir, latestDir, latest_tempDir, 200);

//		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("Pubmed.GenNewData",
				rawDataXXXXObjectDir, latest_tempDir, new_data_xxxxobject, 200);
//

		
		// 以A层格式导出db3
		  JobNode Std2Db3A = JobNodeModel.getJobNode4Std2Db3("pubme_qk.Std2Db3A", "simple.jobstream.mapreduce.common.vip.Std2Db3A",new_data_xxxxobject, 
				  new_data_stdDir, 
		             "base_obj_meta_a_qk.pubmed", 
		             "/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3", 
		              100);
		  
			
//			// 导出数据到 db3——智图
			JobNode StdXXXXObjectZhiTu = JobNodeModel.getJobNode4Std2Db3("pubmed.StdZhiTu",
					"simple.jobstream.mapreduce.site.pubmed.StdPubmedZhiTu",new_data_xxxxobject, new_data_stdDir_zhitu,
					"pubmedjournal", "/RawData/_rel_file/zt_template.db3", 4);
		// 备份累积数据
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("pubmedjournal.Temp2Latest", latest_tempDir,
				latestDir);

		// 正常更新
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(Std2Db3A);
		Std2Db3A.addChildJob(StdXXXXObjectZhiTu);
		StdXXXXObjectZhiTu.addChildJob(Temp2Latest);
//		result.add(Temp2Latest);
////		
		// 测试使用
//		JobNode Json2XXXXObjectTest = JobNodeModel.getJobNode4Parse2XXXXObject("Pubmed.Json2XXXXObject",
//				DateTimeHelper.getNowTimeAsBatch(), "simple.jobstream.mapreduce.site.pubmed.Json2XXXXObjectNew",
//				rawDataDir, rawDataXXXXObjectDir, 200);
//		
//		  JobNode Std2Db3Atest = JobNodeModel.getJobNode4Std2Db3("pubme_qk.Std2Db3A", "simple.jobstream.mapreduce.common.vip.Std2Db3A",rawDataXXXXObjectDir, 
//				  new_data_stdDir, 
//		             "base_obj_meta_a_qk.pubmed", 
//		             "/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3", 
//		              1);
////		 //单次测试
//		result.add(Json2XXXXObjectTest);
//		Json2XXXXObjectTest.addChildJob(Std2Db3Atest);
//		
		// 只导出db3
//		JobNode StdXXXXObjectCheck = JobNodeModel.getJobNode4Std2Db3("pubmed.Std",
//		"simple.jobstream.mapreduce.site.pubmed.StdPubmedCheck", "/RawData/pubmed/xxxxobject", CheckstdDir,
//		"pubmedjournal", "/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3", 1);
//		result.add(StdXXXXObjectCheck);
//		
		// 测试xxx
////
//		String step1_test = "simple.jobstream.mapreduce.site.pubmed.test2";
//		JobNode Test = new JobNode("step1", defaultRootDir, 0, step1_test);
//		result.add(Test);


		
		// 提出国家与语言
//		JobNode PubmedParseTest = new JobNode("parseJob", defaultRootDir, 0,
//				"simple.jobstream.mapreduce.site.pubmed.parseCountry");
//		PubmedParseTest.setConfig("inputHdfsPath", "/RawData/pubmed/big_json/2019/20190129");
//		PubmedParseTest.setConfig("outputHdfsPath", "/user/qianjun/output/pubmed_new");
//		result.add(PubmedParseTest);

		return result;
	}

}
