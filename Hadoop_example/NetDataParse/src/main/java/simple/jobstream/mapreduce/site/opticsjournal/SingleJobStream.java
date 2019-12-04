package simple.jobstream.mapreduce.site.opticsjournal;

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
		String rawDataDir = "/RawData/opticsjournal/big_json/2019/20190614";
		String rawDataXXXXObjectDir = "/RawData/opticsjournal/xxxxobject"; // 新数据解析后的XXXXObject格式数据路径
//		String rawDataXXXXObjectDir ="/RawData/pubmed/xxxtest";   // 临时测试使用
		String latest_tempDir = "/RawData/opticsjournal/latest_temp"; // 新数据和旧数据合并去重得到临时数据的存放路径
		String latestDir = "/RawData/opticsjournal/latest"; // 将latest_tempDir数据保存到该目录，以备下次更新使用

		String new_data_xxxxobject = "/RawData/opticsjournal/new_data/xxxxobject"; // 去重后得到的新数据
		String new_data_stdDir = "/RawData/opticsjournal/new_data/stdfile"; // 新数据转换为DB3格式存放目录
//		String TeststdDir = "/user/qianjun/output/pubmed";
//		String CheckstdDir = "/user/qianjun/output/pubmedtest";
		String  new_data_stdDir_zhitu="/RawData/opticsjournal/zhituDate/stdfile";
		

		// 第一步：将最近增加的big_htm文件转换为xxxxobjcet文件存放在xxxxobject目录下
		JobNode Html2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("opticsjournal.Html2XXXXObject",
				DateTimeHelper.getNowTimeAsBatch(), "simple.jobstream.mapreduce.site.opticsjournal.Html2XXXXObject",
				rawDataDir, rawDataXXXXObjectDir, 3);

		// 将历史累积数据和新数据合并去重

		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("Pubmed.MergeXXXXObject2Temp",
				rawDataXXXXObjectDir, latestDir, latest_tempDir, 200);

//		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("Pubmed.GenNewData",
				rawDataXXXXObjectDir, latest_tempDir, new_data_xxxxobject, 200);
//

		
		// 以A层格式导出db3
		  JobNode Std2Db3A = JobNodeModel.getJobNode4Std2Db3("optics.Std2Db3A", "simple.jobstream.mapreduce.common.vip.Std2Db3A", rawDataXXXXObjectDir, 
				  new_data_stdDir_zhitu, 
		             "opticsJournal_a", 
		             "/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3", 
		              1);
		  
		//  以智图格式导出db3
		  JobNode StdXXXXObjecttest = JobNodeModel.getJobNode4Std2Db3("opticsJournal.Std",
		  "simple.jobstream.mapreduce.site.opticsjournal.StdPubmedZhiTu", rawDataXXXXObjectDir, new_data_stdDir_zhitu,
		  "opticsJournal_zt", "/RawData/_rel_file/zt_template.db3", 1);
		  
		  
		  
		// 备份累积数据
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("pubmedjournal.Temp2Latest", latest_tempDir,
				latestDir);

		// 正常更新
		//生成中间文件
		result.add(Html2XXXXObject);
		//生成a层db3
//		Html2XXXXObject.addChildJob(Std2Db3A);
//		//生成zt db3
//		result.add(StdXXXXObjecttest);
		Html2XXXXObject.addChildJob(StdXXXXObjecttest);
		return result;
	}

}
