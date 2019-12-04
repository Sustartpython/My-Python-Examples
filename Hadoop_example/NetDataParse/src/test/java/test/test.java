package test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.junit.Test;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;
import com.sun.jersey.core.impl.provider.entity.SourceProvider;

public class test {

	/*
	@Test
	public void test() {
		System.out.println("xxxxxxxxxxxxxx");
		fail("Not yet implemented");
	}
	*/
	
	@Test
	public void testParseArticleTitle() {
		simple.jobstream.mapreduce.user.walker.sd_qk.Json2XXXXObject.ProcessMapper obj =  
				new simple.jobstream.mapreduce.user.walker.sd_qk.Json2XXXXObject.ProcessMapper();
		obj.initMapMonth();
		
		
		// https://www.sciencedirect.com/science/article/pii/S0252960217300486
	    // https://www.sciencedirect.com/science/article/abs/pii/S0021929019301150
		String rawid = "S0012369215391558";
//		rawid = "S2212671614001036";
		
//		String url = "http://www.sciencedirect.com/science/article/pii/" + rawid;
		
		System.out.println("rawid:" + rawid);
		try {
			//Document doc = Jsoup.connect(url).get();			
			Document doc = Jsoup.parse(new File("C:\\Users\\walker\\Desktop\\" + rawid + ".html"), "UTF-8");
			
			obj.parseArticleTitle(rawid, doc);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
//	@Test
//	public void testParseSvTitle() {
//		Html2XXXXObject.ProcessMapper obj =  new Html2XXXXObject.ProcessMapper();
//		obj.initMapMonth();
//		
//		String rawid = "S2405844017302797";
//		rawid = "S0002817717308711";
//		String url = "http://www.sciencedirect.com/science/article/pii/" + rawid;
//		
//		System.out.println("rawid:" + rawid);
//		try {
//			//Document doc = Jsoup.connect(url).get();
//			Document doc = Jsoup.parse(new File("C:\\Users\\walker\\Desktop\\"  + rawid + ".html"), "UTF-8");
//			obj.parseSvTitle(rawid, doc);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
////	}

	public String readToString(String fileName) {  
        String encoding = "UTF-8";  
        File file = new File(fileName);  
        Long filelength = file.length();  
        byte[] filecontent = new byte[filelength.intValue()];  
        try {  
            FileInputStream in = new FileInputStream(file);  
            in.read(filecontent);  
            in.close();  
        } catch (FileNotFoundException e) {  
            e.printStackTrace();  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
        try {  
            return new String(filecontent, encoding);  
        } catch (UnsupportedEncodingException e) {  
            System.err.println("The OS does not support " + encoding);  
            e.printStackTrace();  
            return null;  
        }  
    }


	@Test
	public void testParseWFQK() {		
		simple.jobstream.mapreduce.user.walker.wf_qk.Json2XXXXObject.ProcessMapper obj = 
				new simple.jobstream.mapreduce.user.walker.wf_qk.Json2XXXXObject.ProcessMapper();		
		String issueLine = readToString("C:\\Users\\walker\\Desktop\\n.json");
		//System.out.println("issueLine:" + issueLine);
		
		Gson gson = new Gson();
		Type type = new TypeToken<Map<String, JsonElement>>(){}.getType();
		Map<String, JsonElement> mapField = gson.fromJson(issueLine, type);	
		
		JsonArray pageRowJsonArray = mapField.get("pageRow").getAsJsonArray();
		
		for (JsonElement articleJsonElement : pageRowJsonArray) {			
			obj.parseArticle(articleJsonElement);
		}
	}
	
//	@Test
//	public void testParseWFQKRef() {		
//		Json2XXXXObject.ProcessMapper obj = new Json2XXXXObject.ProcessMapper();		
//		String line = readToString("C:\\Users\\walker\\Desktop\\issue.json");
//		//System.out.println("issueLine:" + issueLine);
//		
//		Gson gson = new Gson();
//		Type type = new TypeToken<Map<String, JsonElement>>(){}.getType();
//		Map<String, JsonElement> mapField = gson.fromJson(line, type);	
//		
//		System.out.println("*article_id" + mapField.get("article_id").getAsString());
//		
//		JsonArray refJsonArray = mapField.get("ref").getAsJsonArray();
//		
//		for (JsonElement refJsonElement : refJsonArray) {			
//			//System.out.println(refJsonElement);
//			obj.procOneItem(refJsonElement);
//		}
//	}
//	
//	@Test
//	public void testParseCnkiQK() {		
//		simple.jobstream.mapreduce.user.walker.cnki_qk.Json2XXXXObject.ProcessMapper obj = 
//				new simple.jobstream.mapreduce.user.walker.cnki_qk.Json2XXXXObject.ProcessMapper();		
//		String rawid = "DXFL201000009";
//		rawid = "ABEY201001009";
//		
//		System.out.println("rawid:" + rawid);
//		String htmlText = readToString("C:\\Users\\walker\\Desktop\\" + rawid + ".html");
//			
//		try {			
//			obj.parseKNS(htmlText);
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
//	
//	@Test
//	public void testParseCnkiQKRef() {		
//		Html2XXXXObject.ProcessMapper obj = new Html2XXXXObject.ProcessMapper();		
//		String rawid = "zjkb201606006";
//		rawid = "jjyj201705014";
//		
//		String htmlText = readToString("C:\\Users\\walker\\Desktop\\" + rawid + ".html");
//			
//		try {			
//			Document doc = Jsoup.parse(htmlText);
//	    	for (Element liEle : doc.select("li")) {
//				obj.procOneLi(liEle);
//			}
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
	
//	@Test
//	public void testParseIOP() {		
//		Json2XXXXObject.ProcessMapper obj = new Json2XXXXObject.ProcessMapper();		
//		String rawid = "L114";
//		
//		System.out.println("******************* rawid: " + rawid);
//		String htmlText = readToString("C:\\Users\\walker\\Desktop\\" + rawid + ".htm");
//			
//		try {			
//			Document doc = Jsoup.parse(htmlText);
//			obj.setup(null);
//	    	obj.parseHtml(rawid, doc);
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
	
//	@Test
//	public void testOther() {
//		System.out.println(DateTimeHelper.getNowDate());
//		System.out.println(DateTimeHelper.getParseTime());
//	}
//	
//	public static String hex2Str(String str) throws UnsupportedEncodingException {
//		String strArr[] = str.split("\\\\"); // 分割拿到形如 xE9 的16进制数据
//		byte[] byteArr = new byte[strArr.length - 1];
//		for (int i = 1; i < strArr.length; i++) {
//			Integer hexInt = Integer.decode("0" + strArr[i]);
//			byteArr[i - 1] = hexInt.byteValue();
//		}
//
//		return new String(byteArr, "UTF-8");
//	}
//	
//	public static byte hexToByte(String inHex){  
//		   return (byte)Integer.parseInt(inHex,16);  
//		}  
//	
//	public static byte[] hexToByteArray(String inHex){  
//	    int hexlen = inHex.length();  
//	    byte[] result;  
//	    if (hexlen % 2 == 1){  
//	        //奇数  
//	        hexlen++;  
//	        result = new byte[(hexlen/2)];  
//	        inHex="0"+inHex;  
//	    }else {  
//	        //偶数  
//	        result = new byte[(hexlen/2)];  
//	    }  
//	    int j=0;  
//	    for (int i = 0; i < hexlen; i+=2){  
//	        result[j]=hexToByte(inHex.substring(i,i+2));  
//	        j++;  
//	    }  
//	    return result;   
//	}  
	
//	@Test
//	public void testParseACS() {		
//		simple.jobstream.mapreduce.site.acsjournal.Json2XXXXObject.ProcessMapper obj = 
//				new simple.jobstream.mapreduce.site.acsjournal.Json2XXXXObject.ProcessMapper();		
//		String rawid = "jp060627p";
//		
//		System.out.println("******************* rawid: " + rawid);
//		String htmlText = readToString("C:\\Users\\vip\\Desktop\\" + rawid + ".html");
//			
//		try {			
//			Document doc = Jsoup.parse(htmlText);
//			obj.setup(null);
//	    	obj.parseHtml(doc);
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
	
//	@Test
//	public void testParseCEI() {		
//		simple.jobstream.mapreduce.site.govceiinfo.Json2XXXXObject.ProcessMapper obj = 
//				new simple.jobstream.mapreduce.site.govceiinfo.Json2XXXXObject.ProcessMapper();		
//		String rawid = "1047";
//		
//		System.out.println("******************* rawid: " + rawid);
//		String htmlText = readToString("C:\\Users\\vip\\Desktop\\" + rawid + ".html");
//			
//		try {			
//			Document doc = Jsoup.parse(htmlText);
//			obj.setup(null);
//	    	obj.parseHtml(doc);
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
}




