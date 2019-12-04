package simple.jobstream.mapreduce.user.walker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
//import com.spreada.utils.chinese.ZHConverter;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.charset.Charset;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.httpclient.URI;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.validator.routines.checkdigit.VerhoeffCheckDigit;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import com.process.frame.base.BasicObject.XXXXObject;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.util.StringHelper;
import simple.jobstream.mapreduce.common.util.URLHelper;
import simple.jobstream.mapreduce.common.vip.AuthorOrgan;

public class AppTester {

	
	public static void test17() {
		String cited_cnt = "0";
		
		if (cited_cnt.indexOf('@') < 0) {
			boolean isMatch = Pattern.matches("^\\d+$", cited_cnt);
			if (isMatch) {
				cited_cnt = cited_cnt + "@20190101";
			}
			else {
				cited_cnt = "";
			}
		}
		System.out.println("cited_cnt:" + cited_cnt);
	}
	
	static void test18() {
		String description = "All rights reserved.";
		description = description.substring(0, description.length()  - "All rights reserved.".length());
		System.out.println(description);
	}

	public static void main(String[] args) throws UnsupportedEncodingException {
		test17();
	}
}
