package simple.jobstream.mapreduce.site.cqjtubook;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String rootDir = "";
		String defaultRootDir="";

		// // 带解析新数据路径test" /RawData/cqjtubook/big_json/2019/20190422/8
		String rawDataDir = "/RawData/cqjtubook/big_json/2019/20190422/8,/RawData/cqjtubook/big_json/2019/20190422/7,/RawData/cqjtubook/big_json/2019/20190422/6,/RawData/cqjtubook/big_json/2019/20190422/5,/RawData/cqjtubook/big_json/2019/20190422/4,/RawData/cqjtubook/big_json/2019/20190422/3,/RawData/cqjtubook/big_json/2019/20190422/2,/RawData/cqjtubook/big_json/2019/20190422/1";
		//String rawDataDir = "/RawData/cqjtubook/big_json/2019/20190422/1";
		String rawDataXXXXObjectDir = "/RawData/cqjtubook/xxxxobject"; // 新数据解析后的XXXXObject格式数据路径
//		String rawDataXXXXObjectDir ="/RawData/pubmed/xxxtest";   // 临时测试使用
		String latest_tempDir = "/RawData/cqjtubook/latest_temp"; // 新数据和旧数据合并去重得到临时数据的存放路径
		String latestDir = "/RawData/cqjtubook/latest"; // 将latest_tempDir数据保存到该目录，以备下次更新使用
		String new_data_xxxxobject = "/RawData/cqjtubook/new_data/xxxxobject"; // 去重后得到的新数据
		String new_data_stdDir = "/RawData/cqjtubook/new_data/stdfile"; // 新数据转换为DB3格式存放目录		

		// 第一步：将最近增加的big_htm文件转换为xxxxobjcet文件存放在xxxxobject目录下
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("cqjtubook.Json2XXXXObject",
				DateTimeHelper.getNowTimeAsBatch(), "simple.jobstream.mapreduce.site.cqjtubook.Json2XXXXObject",
				rawDataDir, rawDataXXXXObjectDir, 200);

		// 将历史累积数据和新数据合并去重

		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("cqjtubook.MergeXXXXObject2Temp",
				rawDataXXXXObjectDir, latestDir, latest_tempDir, 200);

//		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("cqjtubook.GenNewData",
				rawDataXXXXObjectDir, latest_tempDir, new_data_xxxxobject, 200);
//
		// 生成智图
		 JobNode Std2Db3 = JobNodeModel.getJobNode4Std2Db3("cqjtubook.Std2Db3", "simple.jobstream.mapreduce.common.vip.Std2Db3A",new_data_xxxxobject, 
				  new_data_stdDir, 
		             "cqjtubook", 
		             "/RawData/_rel_file/zt_template.db3", 
		              1);
			// 生成智图
		 JobNode Std2Db3Test = JobNodeModel.getJobNode4Std2Db3("cqjtubook.Std2Db3", "simple.jobstream.mapreduce.common.vip.Std2Db3A",rawDataXXXXObjectDir, 
				  new_data_stdDir, 
		             "cqjtubook", 
		             "/RawData/_rel_file/zt_template.db3", 
		              1);
		// 备份累积数据
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("cqjtubookjournal.Temp2Latest", latest_tempDir,
				latestDir);

		// 正常更新
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(Std2Db3);
		Std2Db3.addChildJob(Temp2Latest);

		//测试时，生成两步
//		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(Std2Db3);
//		
		return result;
	}

}
