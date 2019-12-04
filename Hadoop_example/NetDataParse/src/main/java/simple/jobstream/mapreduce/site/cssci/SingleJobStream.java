package simple.jobstream.mapreduce.site.cssci;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		
		String rawHtmlDir = "/RawData/cssci/big_json/2019/20190528";
		String metaXXXXObjectDir = "/RawData/cssci/meta/XXXXObject";
		String metaMerRefXXXXObjectDir = "/RawData/cssci/meta/meargerefXXXXObject";
		String metalatest_tempDir = "/RawData/cssci/meta/latest_temp";	//临时成品目录
		String metalatestDir = "/RawData/cssci/meta/latest";	//成品目录
		String metanewXXXXObjectDir = "/RawData/cssci/meta/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String metastdDir = "/RawData/cssci/meta/new_data/StdMeta";
		
		String refXXXXObjectDir = "/RawData/cssci/ref/XXXXObject";
		String reflatest_tempDir = "/RawData/cssci/ref/latest_temp";	//临时成品目录
		String reflatestDir = "/RawData/cssci/ref/latest";	//成品目录
		String refnewXXXXObjectDir = "/RawData/cssci/ref/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String refstdDir = "/RawData/cssci/ref/new_data/StdRef";
		
		
		

		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("csscimeta.Parse", DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.cssci.Json2XXXXObject", rawHtmlDir, metaXXXXObjectDir, 50);
		
		JobNode MergeXXXXObject2Temp  = JobNodeModel.getJonNode4MergeXXXXObject("csscimeta.Merge", metaMerRefXXXXObjectDir, metalatestDir, metalatest_tempDir, 50);
		
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("csscimeta.Extract", metaMerRefXXXXObjectDir, metalatest_tempDir, metanewXXXXObjectDir, 50);
		
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("csscimeta.Copy", metalatest_tempDir, metalatestDir);
		
		JobNode First2Latest = JobNodeModel.getJobNode4CopyXXXXObject("csscimeta.Copy", metalatest_tempDir, metalatestDir);
		
		JobNode StdDb3 = JobNodeModel.getJobNode4Std2Db3("csscimeta.Std","simple.jobstream.mapreduce.common.vip.Std2Db3A", 
				metanewXXXXObjectDir, metastdDir, "csscimeta","/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3",1);
		
		JobNode StdNew = JobNodeModel.getJobNode4Std2Db3("csscimeta.Std","simple.jobstream.mapreduce.site.cssci.StdCsscimeta", 
				metalatest_tempDir, metastdDir, "csscimeta","/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3",5);
		
		JobNode Json2RefXXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("cssciref.Parse", DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.cssci.Json2RefXXXXObject", rawHtmlDir, refXXXXObjectDir, 50);
		
		JobNode MergeRefXXXXObject2Temp  = JobNodeModel.getJonNode4MergeXXXXObject4Ref("cssciref.Merge", refXXXXObjectDir, reflatestDir, reflatest_tempDir, 100);
		
		JobNode GenRefNewData = JobNodeModel.getJonNode4ExtractXXXXObject4Ref("cssciref.Extract", refXXXXObjectDir, reflatest_tempDir, refnewXXXXObjectDir, 50);
		
		JobNode StdRef = JobNodeModel.getJobNode4Std2Db3("cssciref.Std","simple.jobstream.mapreduce.common.vip.Std2Db3A", 
				refnewXXXXObjectDir, refstdDir, "cssciref","/RawData/_rel_file/base_obj_ref_a_template.db3",5);
		
		JobNode MergeRefidCitedid = JobNodeModel.getJobNode4MergeRefidCitedid("csscimeta.Rewrite", refXXXXObjectDir, "",metaXXXXObjectDir, metaMerRefXXXXObjectDir, 50);
		
		JobNode Ref2Latest = JobNodeModel.getJobNode4CopyXXXXObject("cssciref.Copy", reflatest_tempDir, reflatestDir);
		
		
		
		
		
		//*
		//正常更新
		
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(Json2RefXXXXObject);
		Json2RefXXXXObject.addChildJob(MergeRefidCitedid);
		MergeRefidCitedid.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdDb3);
		StdDb3.addChildJob(Temp2Latest);
		Temp2Latest.addChildJob(MergeRefXXXXObject2Temp);
		MergeRefXXXXObject2Temp.addChildJob(GenRefNewData);
		GenRefNewData.addChildJob(StdRef);
		StdRef.addChildJob(Ref2Latest);
		
		
		
		return result;
	}
}
