package com.hqbhoho.bigdata.hive.hooks;

import org.apache.hadoop.hive.ql.tools.LineageInfo;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/21
 */
public class LineageTest {
    public static void main(String[] args) throws Exception {
        String query = "INSERT OVERWRITE TABLE cxy7_dw.tmp_zone_info PARTITION (dt='20171109') SELECT z.zoneid AS zone_id,z.zonename AS zone_name, c.cityid AS city_id, c.cityname AS city_name FROM dict_zoneinfo z LEFT JOIN dict_cityinfo c ON z.cityid = c.cityid AND z.dt='20171109' AND c.dt='20171109' WHERE z.dt='20171109' AND c.dt='20171109'";
        LineageInfo.main(new String[] { query });
    }
}
