package simple.jobstream.mapreduce.site.wfbs;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		
		 String rawDataDir = "/RawData/wanfang/bs/big_htm/big_htm_20190705"; //带解析新数据路径
		 String rawDataXXXXObjectDir = "/RawData/wanfang/bs/xxxxobject"; //新数据解析后的XXXXObject格式数据路径
		 String latest_tempDir = "/RawData/wanfang/bs/latest_temp"; //新数据和旧数据合并去重得到临时数据的存放路径
//		 String latestDir = "/RawData/wanfang/bs/latest"; //A层latest
		 String latestDir = "/RawData/wanfang/bs/latest_bak_20190530";// 智图
		 String new_data_xxxxobject = "/RawData/wanfang/bs/new_data/xxxxobject"; //去重后得到的新数据
		 String new_data_stdDir = "/RawData/wanfang/bs/new_data/stdBS"; //新数据转换为DB3格式存放目录
		 String new_data_stdDir_zt = "/RawData/wanfang/bs/new_data/stdZL"; //新数据转换为DB3格式存放目录
		 String stdDir_ZT="/RawData/wanfang/bs/new_data/std_ZT"; //输出智图DB3目录
		 String step1_html2xxxxobject_class = "simple.jobstream.mapreduce.site.wfbs.Html2XXXXObject";
		 String step2_merge_xxxxobject2temp_class = "simple.jobstream.mapreduce.site.wfbs.MergeXXXXObject2Temp";
		 String step3_gennewdata_class = "simple.jobstream.mapreduce.site.wfbs.GenNewData";
		 String step4_stdxxxxobject_class = "simple.jobstream.mapreduce.site.wfbs.StdXXXXObject";
		 String step5_stdxxxxobject_class = "simple.jobstream.mapreduce.site.wfbs.StdWFBS2ZT";
		 String step6_temp2latest_class = "simple.jobstream.mapreduce.site.wfbs.Temp2Latest";
		
		//第一步：将最近增加的big_htm文件转换为xxxxobjcet文件存放在xxxxobject目录下
//		JobNode Html2XXXXObject = new JobNode("Step_1", defaultRootDir,0, step1_html2xxxxobject_class);
//		Html2XXXXObject.setConfig("inputHdfsPath", rawDataDir);
//		Html2XXXXObject.setConfig("outputHdfsPath", rawDataXXXXObjectDir);
//		
//		//第二步：将旧数据和新增数据合并去重
//		JobNode MergeEIXXXXObject2Temp  = new JobNode("Step_2", defaultRootDir, 0, step2_merge_xxxxobject2temp_class);
//		MergeEIXXXXObject2Temp.setConfig("inputHdfsPath", rawDataXXXXObjectDir + "," + latestDir);
//		MergeEIXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);
//		
//		//第三步：生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
//		JobNode GenNewData = new JobNode("Step_3", defaultRootDir, 0, step3_gennewdata_class);
//		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawDataXXXXObjectDir);
//		GenNewData.setConfig("outputHdfsPath", new_data_xxxxobject);
//		
//		//第四步：将新增的xxxxobject转化为智立方db3数据
//		JobNode StdXXXXObject = new JobNode("Step_4", defaultRootDir, 0, step4_stdxxxxobject_class);
//		StdXXXXObject.setConfig("inputHdfsPath", new_data_xxxxobject);
//		StdXXXXObject.setConfig("outputHdfsPath", new_data_stdDir);
//		
//		JobNode StdXXXXObjectzt = new JobNode("Step_5", defaultRootDir, 0, step5_stdxxxxobject_class);
//		StdXXXXObjectzt.setConfig("inputHdfsPath", rawDataXXXXObjectDir);
//		StdXXXXObjectzt.setConfig("outputHdfsPath", "/RawData/wanfang/bs/new_data/std_ZT");
//		
//		//第五步：备份累积数据
//		JobNode Temp2Latest = new JobNode("Step_6", defaultRootDir, 0, step6_temp2latest_class);
//		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
//		Temp2Latest.setConfig("outputHdfsPath", latestDir);
//		
		// 第一步：解析json,获得数据
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("wanfangbs.Json2XXXXObject",
				DateTimeHelper.getNowTimeAsBatch(), "simple.jobstream.mapreduce.site.wfbs.Html2XXXXObject",
				rawDataDir, rawDataXXXXObjectDir, 20);

		// 第二步：将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("wanfangbs.MergeXXXXObject2Temp",
				rawDataXXXXObjectDir, latestDir, latest_tempDir, 200);

		// 第三步：生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("wanfangbs.GenNewData", rawDataXXXXObjectDir,
				latest_tempDir, new_data_xxxxobject, 200);

		// 第四步：生成A层
//		JobNode Std2Db3A = JobNodeModel.getJobNode4Std2Db3("cnkibs.Std2Db3A",
//				"simple.jobstream.mapreduce.common.vip.Std2Db3A", new_data_xxxxobject, new_data_stdDir,
//				"base_obj_meta_a_qk.cnkibs", "/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3", 1);
		
		// 第四步：生成智图
		JobNode StdXXXXObjectzt = new JobNode("Step_5", defaultRootDir, 0, step5_stdxxxxobject_class);
		StdXXXXObjectzt.setConfig("inputHdfsPath", latestDir);
		StdXXXXObjectzt.setConfig("outputHdfsPath", stdDir_ZT);
//		result.add(StdXXXXObjectzt);

		// 第五步：备份累积数据
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("wanfangbs.Temp2Latest", latest_tempDir, latestDir);
//
//		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
//		MergeXXXXObject2Temp.addChildJob(GenNewData);
//		GenNewData.addChildJob(StdXXXXObjectzt);
//		StdXXXXObjectzt.addChildJob(Temp2Latest);
		
		
		//添加任务结点
//		result.add(Html2XXXXObject);
//		Html2XXXXObject.addChildJob(StdXXXXObjectzt);
//		
////		result.add(Html2XXXXObject);
////		Html2XXXXObject.addChildJob(MergeEIXXXXObject2Temp);
////		MergeEIXXXXObject2Temp.addChildJob(GenNewData);
////		GenNewData.addChildJob(StdXXXXObject);
////		StdXXXXObject.addChildJob(StdXXXXObjectzt);
////		StdXXXXObjectzt.addChildJob(Temp2Latest);
//		
//		// 将latest中的xxxx直接导成A层xxxx
//		String AtoZhiTu = "simple.jobstream.mapreduce.site.wfbs.ZhituXXXXObiectForA";
//		String newLatest= "/RawData/wanfang/bs/latestA";
//		String stdA = "/RawData/wanfang/bs/new_data/std_A";
//		JobNode test_oldToA = new JobNode("oldChangA", defaultRootDir, 0,AtoZhiTu);
//		test_oldToA.setConfig("inputHdfsPath", latestDir);
//		test_oldToA.setConfig("outputHdfsPath", newLatest);
//		
//		// 将A层直接导出A层格式db3
//		JobNode Std2Db3Atest = JobNodeModel.getJobNode4Std2Db3("wanfang_bs.Std2Db3A", "simple.jobstream.mapreduce.common.vip.Std2Db3A","/RawData/wanfang/bs/latestA", 
//				  "/RawData/wanfang/bs/new_data/std_A", 
//		           "base_obj_meta_a_bs.wanfang_bs", 
//		           "/RawData/_rel_file/base_obj_meta_a_template_dt.db3", 
//		            15);
//		result.add(test_oldToA);
//		test_oldToA.addChildJob(Std2Db3Atest);
		/* 导出latest

		StdXXXXObject.setConfig("inputHdfsPath", rawDataXXXXObjectDir);
		result.add(StdXXXXObject);
		//*/
//		result.add(Std2Db3Atest);
		
//		result.add(StdXXXXObjectzt);
		
//		String AtoZhiTu = "simple.jobstream.mapreduce.site.wfbs.StdWFBS";
//	
//		JobNode StdZlf = new JobNode("wfbs_Zlf", defaultRootDir, 0,AtoZhiTu);
//		StdZlf.setConfig("inputHdfsPath", latestDir);
//		StdZlf.setConfig("outputHdfsPath", "/user/ganruoxun/Temp_DB3/wanfang_bs");
//		result.add(StdZlf);
		
		String ExportId= "simple.jobstream.mapreduce.site.wfbs.ExoprtID";
		JobNode ID= new JobNode("wanfangbs_ID", defaultRootDir, 0, ExportId);
		result.add(ID);
//		
		return result;
	}
}
