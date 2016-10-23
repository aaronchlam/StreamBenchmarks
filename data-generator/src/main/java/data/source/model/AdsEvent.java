package data.source.model;

/**
 * Created by jeka01 on 02/09/16.
 */

import org.fluttercode.datafactory.impl.DataFactory;
import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

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

    public String generateJson() {

        //geo
        String geo = null;
        if (isRandomGeo) {
            geo = geoList[ThreadLocalRandom.current().nextInt(0, geoList.length)];
        } else {
            geoIndex = geoIndex % geoList.length;
            geo = geoList[geoIndex];
            geoIndex++;
        }

        //price
        float minX = 5.0f;
        float maxX = 100.0f;
        float finalX = rand.nextFloat() * (maxX - minX) + minX;
        String price = Float.toString(finalX);

        String json = "{ \"geo\":\"" + geo + "\",\"price\":\"" + price + "\"";

        if (putTs){
            return json +  ",\"ts\": \"" + System.currentTimeMillis() + "\"}";
        } else {
            return json + "}";
        }
    }



}

