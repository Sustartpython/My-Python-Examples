package simple.jobstream.mapreduce.site.scirp;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String rootDir = "";
		// // 带解析新数据路径
		String rawDataDir = "/RawData/scirp/big_json/2019/20190618";
		String rawDataXXXXObjectDir = "/RawData/scirp/xxxxobject"; // 新数据解析后的XXXXObject格式数据路径
		String latest_tempDir = "/RawData/scirp/latest_temp"; // 新数据和旧数据合并去重得到临时数据的存放路径
		String latestDir = "/RawData/scirp/latest"; // 将latest_tempDir数据保存到该目录，以备下次更新使用
		String new_data_xxxxobject = "/RawData/scirp/new_data/xxxxobject"; // 去重后得到的新数据
		String new_data_stdDir = "/RawData/scirp/new_data/stdfile_A"; // 新数据转换为DB3格式存放目录，A层
		String new_data_stdDir_zhitu = "/RawData/scirp/new_data/stdfile_zhitu"; // 新数据转换为DB3格式存放目录,智图

		// 第一步：将最近增加的big_htm文件转换为xxxxobjcet文件存放在xxxxobject目录下
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("scirp.Json2XXXXObject",
				DateTimeHelper.getNowTimeAsBatch(), "simple.jobstream.mapreduce.site.scirp.Html2XXXXObject",
				rawDataDir, rawDataXXXXObjectDir, 10);

		// 将历史累积数据和新数据合并去重

		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("scirp.MergeXXXXObject2Temp",
				rawDataXXXXObjectDir, latestDir, latest_tempDir, 200);

//		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("scirp.GenNewData", rawDataXXXXObjectDir,
				latest_tempDir, new_data_xxxxobject, 200);
//
//		// 导出数据到 db3——智图
		JobNode StdXXXXObjectZhiTu = JobNodeModel.getJobNode4Std2Db3("scirp.Std",
				"simple.jobstream.mapreduce.site.scirp.StdScirp", rawDataXXXXObjectDir, new_data_stdDir_zhitu,
				"Scirp.Journal", "/RawData/_rel_file/zt_template.db3", 1);


		// 直接进入A层
	  JobNode Std2Db3Atest = JobNodeModel.getJobNode4Std2Db3("scirp_qk.Std2Db3A", "simple.jobstream.mapreduce.common.vip.Std2Db3A",new_data_xxxxobject, 
			  new_data_stdDir, 
	             "base_obj_meta_a_qk.scirp", 
	             "/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3", 
	              1);

		// 备份累积数据
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("scirp.Temp2Latest", latest_tempDir, latestDir);

		// 正常更新
//		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
//		MergeXXXXObject2Temp.addChildJob(GenNewData);
//		GenNewData.addChildJob(Std2Db3Atest);
//		Std2Db3Atest.addChildJob(StdXXXXObjectZhiTu);
//		StdXXXXObjectZhiTu.addChildJob(Temp2Latest);


		// 单次测试
//		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(Std2Db3A);
//		Json2XXXXObject.addChildJob(StdXXXXObject);
		JobNode StdXXXXObjectZhiTuAll = JobNodeModel.getJobNode4Std2Db3("scirp.Std",
				"simple.jobstream.mapreduce.site.scirp.StdScirp",latestDir, new_data_stdDir_zhitu,
				"Scirp.Journal", "/RawData/_rel_file/zt_template.db3", 1);

//		result.add(StdXXXXObjectZhiTu);
		result.add(StdXXXXObjectZhiTuAll);
		return result;
	}

}
