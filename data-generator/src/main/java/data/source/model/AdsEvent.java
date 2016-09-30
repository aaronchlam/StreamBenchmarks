package data.source.model;

/**
 * Created by jeka01 on 02/09/16.
 */

import org.fluttercode.datafactory.impl.DataFactory;
import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by jeka01 on 31/08/16.
 */
public class AdsEvent {

    private Random rand = new Random(93285L);
    DataFactory df = new DataFactory();
    private List<String> osList = Arrays.asList("iOS", "Android", "Blackberry", "WindowsPhone");
    private List<String> phoneList = Arrays.asList("iPhone6", "iPhone6S", "Blackberry", "Samsung-Edge", "LG");
    private List<String> geoList = Arrays.asList("AF");

    public JSONObject generateJson(boolean isDummy){
        //date
        Date minDate = df.getDate(2016, 5, 1);
        Date maxDate = new Date();
        Date date =  df.getDateBetween(minDate, maxDate);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSSZ");
        String dateString = dateFormat.format(date);

        //phone
        String phone =  df.getItem(phoneList);

        //os
        String os =  df.getItem(osList);

        // osv
        String osv =  df.getNumberBetween(1,9) + "." +  df.getNumberBetween(1,9);

        // ip
        String ip =  rand.nextInt(256) + "." + rand.nextInt(256) + "." + rand.nextInt(256) + "." + rand.nextInt(256);

        //geo
        String geo =  df.getItem(geoList);

        //sessionID
        String sessionID = Integer.toString (  df.getNumberBetween(1,1000));

        //price
        float minX = 5.0f;
        float maxX = 100.0f;
        if (isDummy){
            minX = -200.0f;
            maxX = -100.0f;
        }
        float finalX = rand.nextFloat() * (maxX - minX) + minX;
        String price = Float.toString(finalX);

        //id
      //  String aid1 = UUID.randomUUID().toString();
        String aid1 = rand.nextDouble()+"";
        String iid = UUID.randomUUID().toString();;

        //json object
        JSONObject json = new JSONObject();
        json.put("date",dateString);

        JSONObject device  = new JSONObject();
        device.put("dt",phone);
        device.put("os",os);
        device.put("osv",osv);
        device.put("ip",ip);
        device.put("geo",geo);

        json.put("t",device);

        JSONObject user  = new JSONObject();
        user.put("iid",iid);
        user.put("aid1",aid1);

        json.put("s",user);

        JSONObject m = new JSONObject();
        m.put("SESSION_ID",sessionID);
        m.put("price",price);

        json.put("m",m);
        json.put("ts",System.currentTimeMillis());
        return json;
    }


//    private Date getDateBetween(Date minDate, Date maxDate) {
//        long seconds = (maxDate.getTime() - minDate.getTime()) / 1000L;
//        seconds = (long)(rand.nextDouble() * (double)seconds);
//        Date result = new Date();
//        result.setTime(minDate.getTime() + seconds * 1000L);
//        return result;
//    }
//
//    public Date getDate(int year, int month, int day) {
//        Calendar cal = Calendar.getInstance();
//        cal.clear();
//        cal.set(year, month - 1, day, 0, 0, 0);
//        return cal.getTime();
//    }
//
//    public <T> T getItem(List<T> items, int probability, T defaultItem) {
//        if(items == null) {
//            throw new IllegalArgumentException("Item list cannot be null");
//        } else if(items.isEmpty()) {
//            throw new IllegalArgumentException("Item list cannot be empty");
//        } else {
//            return this.chance(probability)?items.get(rand.nextInt(items.size())):defaultItem;
//        }
//    }
//    public boolean chance(int chance) {
//        return rand.nextInt(100) < chance;
//    }
//
//    public <T> T getItem(List<T> items) {
//        return getItem(items,100,null);
//    }
//
//    public int getNumberBetween(int min, int max) {
//        if(max < min) {
//            throw new IllegalArgumentException(String.format("Minimum must be less than minimum (min=%d, max=%d)", new Object[]{Integer.valueOf(min), Integer.valueOf(max)}));
//        } else {
//            return min + rand.nextInt(max - min);
//        }
//    }


}

