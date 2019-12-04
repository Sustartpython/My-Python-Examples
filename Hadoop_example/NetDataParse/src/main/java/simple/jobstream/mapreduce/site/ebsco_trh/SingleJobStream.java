package simple.jobstream.mapreduce.site.ebsco_trh;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		String rootDir = "";
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		
		String rawHtmlDir = "/RawData/ebsco/trh_TRH/big_html/20190712";
		String rawXXXXObjectDir = "/RawData/ebsco/trh_TRH/XXXXObject";
		String latest_tempDir = "/RawData/ebsco/trh_TRH/latest_temp"; // 临时成品目录
		String latestDir = "/RawData/ebsco/trh_TRH/latest"; // 成品目录
		String newXXXXObjectDir = "/RawData/ebsco/trh_TRH/new_data/XXXXObject"; // 本次新增的数据（XXXXObject）
		String std_Dir_zt = "/RawData/ebsco/trh_TRH/new_data/zt/StdEBSCO";
		String std_Dir_a = "/RawData/ebsco/trh_TRH/new_data/a/StdEBSCO";
		
		JobNode Html2XXXXObject = new JobNode("ebsco_trh_Parse", rootDir, 0,
				"simple.jobstream.mapreduce.site.ebsco_trh.Html2XXXXObject");
		Html2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Html2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);

		// 将历史累积数据和新数据合并去重
//
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("ebsco_trh_MergeXXXXObject2Temp",
				rawXXXXObjectDir, latestDir, latest_tempDir, 200);

//		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("ebsco_trh_GenNewData", 
				rawXXXXObjectDir, latest_tempDir,newXXXXObjectDir, 200);	
			

		 //导出数据到 db3
		JobNode StdNew = JobNodeModel.getJobNode4Std2Db3("ebsco_trh_zt","simple.jobstream.mapreduce.site.ebsco_trh.StdXXXXobject_zt", 
				newXXXXObjectDir, std_Dir_zt, "zt_ebscotrhjournal","/RawData/_rel_file/zt_template.db3",1);

//		// 备份累积数据
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("ebsco_trh_Temp2Latest", latest_tempDir, latestDir);
		
		//导出数据到a层
		JobNode Std2Db3A = JobNodeModel.getJobNode4Std2Db3("ebsco_trh_a", "simple.jobstream.mapreduce.site.ebsco_trh.StdXXXXobject_a", newXXXXObjectDir, 
				std_Dir_a, 
		             "a_ebscotrhjournal", 
		             "/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3", 
		              1);
		
		//test
//		JobNode StdNew2 = JobNodeModel.getJobNode4Std2Db3("ebsco_trh_zt","simple.jobstream.mapreduce.site.ebsco_trh.StdXXXXobject_zt", 
//				rawXXXXObjectDir, std_Dir_zt, "zt_ebscotrhjournal","/RawData/_rel_file/zt_template.db3",1);
//		JobNode Std2Db3A2 = JobNodeModel.getJobNode4Std2Db3("ebsco_trh_a", "simple.jobstream.mapreduce.site.ebsco_trh.StdXXXXobject_a", rawXXXXObjectDir, 
//				std_Dir_a, 
//		             "a_ebscotrhjournal", 
//		             "/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3", 
//		              1);
//		result.add(Html2XXXXObject);
//		Html2XXXXObject.addChildJob(StdNew2);
//		StdNew2.addChildJob(Std2Db3A2);
		
		
		// 正常更新
		result.add(Html2XXXXObject);
		Html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdNew);
		StdNew.addChildJob(Temp2Latest);
//		Temp2Latest.addChildJob(Std2Db3A);
		
		//从latest读取数据，更改id
//		JobNode StdNew_latest = JobNodeModel.getJobNode4Std2Db3("ebsco.Std","simple.jobstream.mapreduce.site.ebsco_aph.StdXXXXobject_zt", 
//				latestDir, std_Dir_zt, "ebscoaphjournal_zt","/RawData/_rel_file/zt_template.db3",1);
//		JobNode Std2Db3A_latest = JobNodeModel.getJobNode4Std2Db3("ebsco.Std2Db3A", "simple.jobstream.mapreduce.site.ebsco_aph.StdXXXXobject_a", latestDir, 
//				std_Dir_a, 
//		             "ebscoaphjournal_a", 
//		             "/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3", 
//		              1);
//		result.add(StdNew_latest);
//		StdNew_latest.addChildJob(Std2Db3A_latest);
//		String ExportId= "simple.jobstream.mapreduce.site.ebsco_aph.ExoprtID";
//		JobNode ID= new JobNode("ebsco_aph_ID",rootDir, 0, ExportId);
//		StdNew_latest.addChildJob(ID);
		
		
		return result;
	}
}
