
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONValue;
//import net.minidev.json.JSONStyle;
//import net.minidev.json.parser.ParseException;

public class BidRequestParser {
  public String parseBidRequest(String fileInputReq,String bid_price) {
    FileInputStream inputfstream;
    BufferedReader bufferedReader;
    String finalline =null;
    Float bidprice = Float.parseFloat(bid_price.toString());
    try{
      inputfstream = new FileInputStream(fileInputReq);
      bufferedReader = new BufferedReader(new InputStreamReader(inputfstream));
      String readLine = null;
      while((readLine = bufferedReader.readLine()) != null){
        Object obj =JSONValue.parse(readLine);
        JSONObject datajson = (JSONObject) obj;
        // 1
        String auctionid = datajson.containsKey("auction_id") ? (String) datajson.get("auction_id"):null;

        String createdatetime = (String) datajson.get("create_datetime");
        // 2,3
        String[] times = createdatetime.split(" ");
        String[] days = times[0].split("-");
        String[] hrs = times[1].split(":");
        Integer day = Integer.valueOf(days[2]) % 7 ;
        Integer hr = Integer.valueOf(hrs[0]);

        Integer exchangeid = datajson.containsKey("exchange_id") ?(Integer) datajson.get("exchange_id"):0;

        JSONObject secendjson = datajson.containsKey("json") ? (JSONObject) JSONValue.parse((String) datajson.get("json")) : null;

        // app
        JSONObject appjson = secendjson.containsKey("app") ? (JSONObject) secendjson.get("app") : null;

        //5,6,7
        String appid;
        String appcat;
        String apppubliserid ;
        if (appjson != null){
          // app id and cat
          appid = appjson.containsKey("id") ? (String) appjson.get("id"): "0";

          JSONArray appcatjson = appjson.containsKey("cat") ? (JSONArray) appjson.get("cat") : null;
          JSONObject apppubliserjson = appjson.containsKey("publiser") ? (JSONObject) appjson.get("publiser") : null;

          if (appcatjson != null){
            if (appcatjson.size() > 0){
              appcat = (String) appcatjson.get(0);
              for (int i =1 ; i < appcatjson.size(); i++){
                if(!appcat.contains((CharSequence) appcatjson.get(i).toString())){
                  appcat = appcat+","+(String) appcatjson.get(i);
                }
              }
            }else{
              appcat = null;
            }
          }else{
            appcat = null;
          }

          if (apppubliserjson != null){
            apppubliserid = apppubliserjson.containsKey("id") ? (String) apppubliserjson.get("id") :null;
          }else{
            apppubliserid = null;
          }
        }else{
          appid = null;
          appcat = null;
          apppubliserid = null;
        }

        // imp
        JSONArray impjson = secendjson.containsKey("imp") ? (JSONArray) secendjson.get("imp") : null;
        //8
        JSONObject impjsonobj = (JSONObject) impjson.get(0);
        Float impbidfloor =impjsonobj.containsKey("bidfloor") ? Float.parseFloat(impjsonobj.get("bidfloor").toString()) : (float) 0;
        // imp banner
        JSONObject impbannerjson = impjsonobj.containsKey("banner") ? (JSONObject) impjsonobj.get("banner") : null;
        //9,10

        Integer impbannerw;
        Integer impbannerh;
        if (impbannerjson != null){
          impbannerw = impbannerjson.containsKey("w") ? (Integer) impbannerjson.get("w"):0;
          if (impbannerjson.containsKey("h")){
            impbannerh = impbannerjson.containsKey("h") ? (Integer) impbannerjson.get("h"):0;
          }else{
            impbannerw = 0;
            impbannerh = 0;
          }


          JSONArray impbannerbtypejson;
          impbannerbtypejson = impbannerjson.containsKey("btype") ? (JSONArray) impbannerjson.get("btype") :null;

          // 11
          String impbannerbtype;
          if (impbannerbtypejson == null){
            impbannerbtype = null;
          }else{
            impbannerbtype = (String) impbannerbtypejson.get(0).toString();
            for (int i = 1; i< impbannerbtypejson.size(); i++){
              if (!impbannerbtype.contains((CharSequence) impbannerbtypejson.get(i).toString())){
                impbannerbtype =impbannerbtype +","+ (String) impbannerbtypejson.get(i).toString();
              }
            }

          }

          JSONArray impbannermimesjson  = (impbannerjson != null) ? (JSONArray) impbannerjson.get("mimes") : null;
          // 12
          String impbannermimes;
          if (impbannermimesjson != null){
            impbannermimes = (String) impbannermimesjson.get(0);
            for (int i =1 ; i < impbannermimesjson.size(); i++){
              if (!impbannermimes.contains((CharSequence) impbannermimesjson.get(i).toString())){
                impbannermimes = impbannermimes+","+(String) impbannermimesjson.get(i);
              }
            }
          }else{
            impbannermimes = null;
          }

          // devices
          JSONObject devicejson = (JSONObject) secendjson.get("device");
          // devices os
          //13
          String deviceos;
          if (devicejson.containsKey("os")){
            deviceos = (String) devicejson.get("os");
            if (deviceos.equals("Android")){
              deviceos = "1";
            }else if (deviceos.equals("iOS")){
              deviceos = "2";
            }else{
              deviceos = "0";
            }
          }else{
            deviceos = "Unknown";
          }

          // 14
          String deviceOsv = devicejson.containsKey("Osv".toLowerCase()) ? (String) devicejson.get("Osv") : "0";

          // model 15
          String devicemodel = devicejson.containsKey("model") ? (String) devicejson.get("model") : null;
          // 16
          Integer deviceconnectiontype = devicejson.containsKey("connectiontype") ? (Integer) devicejson.get("connectiontype"):0;
          // devices geo
          JSONObject devicegeojson = devicejson.containsKey("geo") ? (JSONObject) devicejson.get("geo"): null;

          // 17
          String devicegeocontry = devicegeojson.containsKey("country") ? (String) devicegeojson.get("country") : null;
          // devices ua 18
          String deviceua = devicejson.containsKey("ua") ? (String) devicejson.get("ua") : null;
          // 19
          String carrierua = devicejson.containsKey("carrier") ? (String) devicejson.get("carrier"):null;
          // 20
          Integer devicejs = devicejson.containsKey("js") ? (Integer) devicejson.get("js") : 0;

          String regularForm;
          String[] operators = {"windows", "ios", "mac", "android", "linux"};
          String[] browsers = {"chrome", "sogou", "maxthon", "safari", "firefox", "theworld", "opera", "ie"};

          String operation = "other";
          String browser = "other";
          if (deviceua != null){
            for (String op : operators){
              if( deviceua.toLowerCase().contains(op)){
                operation = op;
                break;
              }
            }
            for (String br : browsers){
              if(deviceua.toLowerCase().contains(br)){
                browser = br;
                break;
              }
            }
            regularForm = operation + "_" + browser;
          }else{
            regularForm = null;
          }

          // ext
          JSONObject extjson = secendjson.containsKey("ext") ? (JSONObject) secendjson.get("ext") :null;

          //21
          String extcarriername;
          if (extjson != null){
            extcarriername = extjson.containsKey("carriername") ? (String) extjson.get("carriername") : "-";
          }else{
            extcarriername = "-";
          }

          // badv and bcat
          //22,23

          JSONArray jsonbadvjson = secendjson.containsKey("badv") ? (JSONArray) secendjson.get("badv") : null;
          String jsonbadv ;
          if(jsonbadvjson != null){
            jsonbadv = (String) jsonbadvjson.get(0);
            Pattern p1 = Pattern.compile("\\s+");
            Matcher m1 = p1.matcher(jsonbadv);
            jsonbadv = m1.replaceAll(",");
            String[] jsonbadvsplit = jsonbadv.split(",");
            String jsonbadvsplit1 = jsonbadvsplit[0];
            for (int i = 1; i<jsonbadvsplit.length; i++){
              if(!jsonbadvsplit1.contains(jsonbadvsplit[i].toString())){
                jsonbadvsplit1 = jsonbadvsplit1 +","+(String) jsonbadvsplit[i];
              }
            }
            jsonbadv = jsonbadvsplit1;
          }else{
            jsonbadv = null;
          }


          JSONArray jsonbcatjson = secendjson.containsKey("bcat") ? (JSONArray) secendjson.get("bcat") : null;
          String jsonbcat;
          if (jsonbcatjson != null){
            jsonbcat = (String) jsonbcatjson.get(0);
            for (int i =1 ; i < jsonbcatjson.size(); i++){
              if (!jsonbcat.contains((CharSequence) jsonbcatjson.get(i).toString())){
                jsonbcat = jsonbcat+","+(String) jsonbcatjson.get(i);
              }
            }
          }else{
            jsonbcat = null;
          }
          // 24
          Integer userjson = secendjson.containsKey("user") ? 1 : 0;

          finalline = day + "\t" + hr +"\t"+exchangeid +"\t" +appid +"\t"+ apppubliserid + "\t"+ impbidfloor +"\t"+impbannerw +"\t"+ impbannerh + "\t"+deviceos+"\t"+deviceOsv +"\t"+ devicemodel + "\t" + deviceconnectiontype +"\t"
          +devicegeocontry +"\t"+ regularForm +"\t" + carrierua +"\t"+devicejs + "\t" + userjson +"\t"+ extcarriername +"\t"+
          appcat +"\t"+ impbannerbtype+"\t"+ impbannermimes +"\t"+ jsonbadv +"\t"+ jsonbcat + "\t" + bidprice;
        }
        return finalline;
      }
    }
