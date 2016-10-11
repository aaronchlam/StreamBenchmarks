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

    public AdsEvent(boolean isRandomGeo, boolean putTs, Double partition) {
        this.isRandomGeo = isRandomGeo;
        this.putTs = putTs;
        if (partition > 0){
            geoList = Arrays.copyOfRange(geoListAll, 0, (int) (geoListAll.length * partition));
        } else{
            geoList = Arrays.copyOfRange(geoListAll, (int) (geoListAll.length * (1 + partition)), geoListAll.length);
        }
    }

    private boolean isRandomGeo;
    private boolean putTs;

    private int geoIndex = 0;
    private Random rand = new Random(93285L);
    //private Random rand = new Random();
    DataFactory df = new DataFactory();
    private List<String> osList = Arrays.asList("iOS", "Android", "Blackberry", "WindowsPhone");
    private List<String> phoneList = Arrays.asList("iPhone6", "iPhone6S", "Blackberry", "Samsung-Edge", "LG");
    private String[] geoListAll = {"AF", "AX", "AL"
            , "DZ", "AS", "AD", "AO", "AI", "AQ", "AG", "AR", "AM",
            "AW", "AC", "AU", "AT", "AZ", "BS", "BH", "BB", "BD", "BY", "BE", "BZ", "BJ", "BM", "BT", "BW", "BO", "BA", "BV", "BR",
            "IO", "BN", "BG", "BF", "BI", "KH", "CM", "CA", "CV", "KY", "CF", "TD", "CL", "CN", "CX", "CC", "CO", "KM", "CG", "CD",
            "CK", "CR", "CI", "HR", "CU", "CY", "CZ", "CS", "DK", "DJ", "DM", "DO", "TP", "EC", "EG", "SV", "GQ", "ER", "EE", "ET",
            "EU", "FK", "FO", "FJ", "FI", "FR", "FX", "GF", "PF", "TF", "MK", "GA", "GM", "GE", "DE", "GH", "GI", "GB", "GR", "GL",
            "GD", "GP", "GU", "GT", "GG", "GN", "GW", "GY", "HT", "HM", "HN", "HK", "HU", "IS", "IN", "ID", "IR", "IQ", "IE", "IL",
            "IM", "IT", "JE", "JM", "JP", "JO", "KZ", "KE", "KI", "KP", "KR", "KW", "KG", "LA", "LV", "LB", "LI", "LR", "LY", "LS",
            "LT", "LU", "MO", "MG", "MW", "MY", "MV", "ML", "MT", "MH", "MQ", "MR", "MU", "YT", "MX", "FM", "MC", "MD", "MN", "ME",
            "MS", "MA", "MZ", "MM", "NA", "NR", "NP", "NL", "AN", "NT", "NC", "NZ", "NI", "NE", "NG", "NU", "NF", "MP", "NO", "OM",
            "PK", "PW", "PS", "PA", "PG", "PY", "PE", "PH", "PN", "PL", "PT", "PR", "QA", "RE", "RO", "RU", "RW", "GS", "KN", "LC",
            "VC", "WS", "SM", "ST", "SA", "SN", "RS", "YU", "SC", "SL", "SG", "SI", "SK", "SB", "SO", "ZA", "ES", "LK", "SH", "PM",
            "SD", "SR", "SJ", "SZ", "SE", "CH", "SY", "TW", "TJ", "TZ", "TH", "TG", "TK", "TO", "TT", "TN", "TR", "TM", "TC", "TV",
            "UG", "UA", "AE", "UK", "US", "UM", "UY", "SU", "UZ", "VU", "VA", "VE", "VN", "VG", "VI", "WF", "EH", "YE", "ZM", "ZR", "ZW"};
    private String[] geoList = null;

    public JSONObject generateJson(boolean isDummy) {
        //date
        Date minDate = df.getDate(2016, 5, 1);
        Date maxDate = new Date();
        Date date = df.getDateBetween(minDate, maxDate);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSSZ");
        String dateString = dateFormat.format(date);

        //phone
        String phone = df.getItem(phoneList);

        //os
        String os = df.getItem(osList);

        // osv
        String osv = df.getNumberBetween(1, 9) + "." + df.getNumberBetween(1, 9);

        // ip
        String ip = rand.nextInt(256) + "." + rand.nextInt(256) + "." + rand.nextInt(256) + "." + rand.nextInt(256);

        //geo
        String geo = null;
        if (isRandomGeo) {
            geo = df.getItem(geoList);
        } else {
            geoIndex = geoIndex % geoList.length;
            geo = geoList[geoIndex];
            geoIndex++;
        }

        //sessionID
        String sessionID = Integer.toString(df.getNumberBetween(1, 1000));

        //price
        float minX = 5.0f;
        float maxX = 100.0f;
        String price = null;
        if (isDummy) {
            price = "-100.0";
        } else {
            float finalX = rand.nextFloat() * (maxX - minX) + minX;
            price = Float.toString(finalX);
        }

        //id
        //  String aid1 = UUID.randomUUID().toString();
        String aid1 = rand.nextDouble() + "";
        String iid = UUID.randomUUID().toString();
        ;

        //json object
        JSONObject json = new JSONObject();
        json.put("date", dateString);

        JSONObject device = new JSONObject();
        device.put("dt", phone);
        device.put("os", os);
        device.put("osv", osv);
        device.put("ip", ip);
        device.put("geo", geo);

        json.put("t", device);

        JSONObject user = new JSONObject();
        user.put("iid", iid);
        user.put("aid1", aid1);

        json.put("s", user);

        JSONObject m = new JSONObject();
        m.put("SESSION_ID", sessionID);
        m.put("price", price);

        json.put("m", m);
        if (putTs) json.put("ts", System.currentTimeMillis());
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

