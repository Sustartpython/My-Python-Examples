package simple.jobstream.mapreduce.site.pubmed;

import java.net.URI;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;

import com.process.frame.JobStreamRun;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

public class ViewXXXObject {
	private static void _view(String ...strings) {
		try {
			Configuration conf = new Configuration();
			for (Entry<Object, Object> entry : JobStreamRun.getCustomProperties().entrySet())
			{
				conf.set(entry.getKey().toString(), entry.getValue().toString());
			}
			URI uri = URI.create(conf.get("fs.default.name"));
			FileSystem hdfs;
			hdfs = FileSystem.get(uri, conf);
			Path path = new Path(strings[0]); 	//文件或目录路径
			
			HashSet<String> fieldSet = new HashSet<String>();
			if (strings.length > 1) {
				for (String field : strings[1].split(";")) {
					fieldSet.add(field.trim());
				}
			}
			
			FileStatus[] files = hdfs.listStatus(path);
			long cntRecord = 0;
			for (FileStatus file : files) {
				System.out.println(file.getPath().getName());
				
				if (file.isDirectory()) {		//忽略叶子目录
					continue;
				}
				
				if (!file.getPath().getName().startsWith("part-")) {	//忽略干扰文件
					continue;
				}

				XXXXObject xObj = new XXXXObject();
		        SequenceFile.Reader in = new SequenceFile.Reader(conf, Reader.file(file.getPath()));
		        
		        Object key = null;
		        Object value = null;
		        
		        
		        while (true) {
		        	key = in.next(key);
		        	if (key == null) {
		        		break;	
		        	}	        	
		        	
		        	value = in.getCurrentValue((Object) value);
		        	VipcloudUtil.DeserializeObject(((BytesWritable)value).getBytes(), xObj);
		        	//System.out.println("keyword_c:" + xObj.data.getOrDefault("keyword_c", "").trim());
//		        	System.out.println("**********************data*******************************");
//		        	System.out.println("*** _key:" + key.toString());
	        		for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
	        			String field = updateItem.getKey().trim();
	        			String fval = updateItem.getValue().trim();
	        			
	        			// fieldSet 为空或包含本字段时输出
	        			System.out.println(field + ":" + fval);
//	        			if (fval.equals("CN00100100.0")) {
//	        				System.out.println(field + ":" + fval);
//	        				return;
//						}
	        			//System.out.println(field + ":" + fval);
	        			
		        	}
	        		cntRecord += 1;
//	        		System.out.println("*** cntRecord: " + cntRecord);
//	        		System.out.println("**********************data*******************************");
//	        		System.out.println("\n\n");
		        	
				}
		        in.close();   
		        
		        System.out.println("*** total: " + cntRecord);
			}
			hdfs.close();

		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
	}

	public static void view(String sPath) {
		_view(sPath);
	}
	
	public static void view(String sPath, String sFilter) {
		_view(sPath, sFilter);
	}
	
	public static void main(String args[]) throws Exception{
		//ViewXXXObject objectDetail = new ViewXXXObject();
		//objectDetail.search("/RawData/cnki/bz/XXXXObject/part-r-00000");
		//objectDetail.search("/RawData/cnki/bz/XXXXObject");
		ViewXXXObject.view("/user/tanjj/jigouku/WosCHina");
	}
}
