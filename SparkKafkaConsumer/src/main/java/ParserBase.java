package KafkaConsumer;

/*
* How to Read JSON Object From File in Java
*/

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.minidev.json.JSONValue;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONArray;
//import net.minidev.json.JSONStyle;
//import net.minidev.json.parser.ParseException;

public class ParserBase {

	public String parseBid(String req) {
		if ("".equals(req.trim())) {
			return "###";
		}

		try {

		} catch (Exception e) {
			return "***" + e.getMessage();
		}
	}

	public String parserNotice(String winnotice) {
		if ("".equals(winnotice.trim())) {
			return "###";
		}
		try{

		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}
}
