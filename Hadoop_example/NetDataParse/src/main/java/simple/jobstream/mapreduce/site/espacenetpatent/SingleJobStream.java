package simple.jobstream.mapreduce.site.espacenetpatent;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.vip.JobNodeModel;



public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		String rootDir = "";
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		
		String rawHtmlDir = "/RawData/epregister/big_html/20160611";
		String rawXXXXObjectDir = "/RawData/epregister/XXXXObject";
		String latest_tempDir = "/RawData/epregister/latest_temp"; // 临时成品目录
		String latestDir = "/RawData/epregister/latest"; // 成品目录
		String newXXXXObjectDir = "/RawData/epregister/new_data/XXXXObject"; // 本次新增的数据（XXXXObject）
		String std_Dir_zt = "/RawData/epregister/new_data/zt/Std_epregister";
		String std_Dir_a = "/RawData/epregister/new_data/a/Std_epregister";
		String nt = "/RawData/espacentpatent/nt2";
		String ntXXXXObject = "/RawData/espacentpatent/ntXXXXXObject";
		String ntdb3 = "/RawData/espacentpatent/ntdb3/std";
		
		JobNode Html2XXXXObject = new JobNode("epregisterParse", rootDir, 0,
				"simple.jobstream.mapreduce.site.epregister.Html2XXXXObject");
		Html2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Html2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);

		// 将历史累积数据和新数据合并去重
//
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("epregister.MergeXXXXObject2Temp",
				rawXXXXObjectDir, latestDir, latest_tempDir, 200);

//		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("epregister.GenNewData", 
				rawXXXXObjectDir, latest_tempDir,newXXXXObjectDir, 200);	
			

		 //导出数据到 db3
		JobNode StdNew = JobNodeModel.getJobNode4Std2Db3("epregister.Std","simple.jobstream.mapreduce.site.epregister.StdXXXXobject_zt", 
				newXXXXObjectDir, std_Dir_zt, "epregister_zt","/RawData/_rel_file/zt_template.db3",1);

//		// 备份累积数据
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("epregister.Temp2Latest", latest_tempDir, latestDir);
		
		//导出数据到a层
		JobNode Std2Db3A = JobNodeModel.getJobNode4Std2Db3("epregister.Std2Db3A", "simple.jobstream.mapreduce.site.epregister.StdXXXXobject_a", newXXXXObjectDir, 
				std_Dir_a, 
		             "epregister_a", 
		             "/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3", 
		              1);

		//读取下载的nt文件，放到db3数据库里
		JobNode nt2XXXXObject = new JobNode("espacenetpatentParse", rootDir, 0,
				"simple.jobstream.mapreduce.site.espacenetpatent.NT2XXXXObject");
		nt2XXXXObject.setConfig("inputHdfsPath", nt);
		nt2XXXXObject.setConfig("outputHdfsPath", ntXXXXObject);
		JobNode ntXXXXObject2db3 = JobNodeModel.getJobNode4Std2Db3("espacenetpatent.Std","simple.jobstream.mapreduce.site.espacenetpatent.NTXXXXObject2db3", 
				ntXXXXObject, ntdb3, "nt","/RawData/espacentpatent/nt_template/nt.db3",1);
		
		
		result.add(nt2XXXXObject);
		nt2XXXXObject.addChildJob(ntXXXXObject2db3);
		
		

		// 正常更新
//		result.add(Html2XXXXObject);
//		Html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
//		MergeXXXXObject2Temp.addChildJob(GenNewData);
//		GenNewData.addChildJob(StdNew);
//		StdNew.addChildJob(Temp2Latest);
//		Temp2Latest.addChildJob(Std2Db3A);
		
		
		//从latest读取数据，更改id
//		JobNode StdNew_latest = JobNodeModel.getJobNode4Std2Db3("ebsco.Std","simple.jobstream.mapreduce.site.ebsco_buh.StdXXXXobject_zt", 
//				latestDir, std_Dir_zt, "ebscobuhjournal_zt","/RawData/_rel_file/zt_template.db3",1);
//		JobNode Std2Db3A_latest = JobNodeModel.getJobNode4Std2Db3("ebsco.Std2Db3A", "simple.jobstream.mapreduce.site.ebsco_buh.StdXXXXobject_a", latestDir, 
//				std_Dir_a, 
//		             "ebscobuhjournal_a", 
//		             "/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3", 
//		              1);
//		result.add(StdNew_latest);
//		StdNew_latest.addChildJob(Std2Db3A_latest);
//		String ExportId= "simple.jobstream.mapreduce.site.ebsco_buh.ExoprtID";
//		JobNode ID= new JobNode("ebsco_buh_ID",rootDir, 0, ExportId);
//		StdNew_latest.addChildJob(ID);
		
	

		return result;
	}
}
