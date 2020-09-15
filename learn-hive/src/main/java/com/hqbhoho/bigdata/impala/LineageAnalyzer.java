package com.hqbhoho.bigdata.impala;

import com.alibaba.fastjson.JSON;
import com.hqbhoho.bigdata.hive.parse1.utils.FileUtils;
import com.hqbhoho.bigdata.hive.parse1.utils.JDBCClient;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * describe:
 * <p>
 * Impala 血缘分析
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/05/22
 */
public class LineageAnalyzer {
    public static void main(String[] args) throws Exception {

        List<String> BEGIN_WORDS = Arrays.asList("CREATE", "INSERT", "ALTER");
        // 获取当前日期的时间戳
        long dayStartTimestamp = LocalDate.now().atStartOfDay().toEpochSecond(ZoneOffset.of("+8"));
        JDBCClient mysqlClient = JDBCClient.getMysqlClient();

        // 获取血缘日志文件
        List<String> paths = FileUtils.traversalFileDirectory(new File("C:\\Users\\13638\\Desktop\\lineage"));
        paths.stream().forEach(
                path -> {
                    BufferedReader bufferReader = null;

                    try {

                        bufferReader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(path))));
                        String line;

                        while ((line = bufferReader.readLine()) != null) {
                            Map info = (Map) JSON.parse(line);
                            Long start_time = Long.valueOf((Integer) info.get("timestamp"));
                            if (start_time >= dayStartTimestamp) {
                                String queryText = ((String) info.get("queryText")).trim().replaceAll("[\\t|\\n|\\r]", " ").toUpperCase();
                                boolean flag = true;
                                // 目前只保留 INSERT语法  CREATE 语法  ALTER 语法
                                for (String word : BEGIN_WORDS) {
                                    int index = queryText.indexOf(word);
                                    if (index >= 0) {
                                        queryText = queryText.substring(index);
                                        flag = false;
                                        break;
                                    }

                                }
                                if (flag) continue;
                                // 对注释进行清理
                                // 去除/* */注释
                                if (queryText.contains("/*") && queryText.contains("*/")) {
                                    queryText = queryText.replaceAll("/\\*.*\\*/", "");
                                }

                                // 去除多个空格  获取较为干净的sql语句
                                queryText = queryText.replaceAll("\\s+", " ");
                                String queryText1 = queryText;
                                // 获取当前日期
                                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
                                String now = formatter.format(LocalDateTime.now());
                                // 获取结束时间
                                Long end_time = Long.valueOf((Integer) info.get("endTime"));
                                Long duration = end_time - start_time;
                                // 开始解析血缘
                                // 顶点
                                List<Map> verts = (List<Map>) info.get("vertices");
                                // 边的关系
                                List<Map> edges = (List<Map>) info.get("edges");
                                Connection conn = mysqlClient.getConnection();
                                edges.stream()
                                        .filter(edge -> "PROJECTION".equalsIgnoreCase((String) edge.get("edgeType")))
                                        .forEach(edge -> {
                                            List<Integer> sources = (List<Integer>) edge.get("sources");
                                            List<Integer> targets = (List<Integer>) edge.get("targets");
                                            for (Integer target_index : targets) {
                                                for (Integer source_index : sources) {
                                                    String target = (String) verts.get(target_index).get("vertexId");
                                                    String source = (String) verts.get(source_index).get("vertexId");
                                                    String sql = "insert into test.t_lineage_relation " +
                                                            "(res_col_id_from," +
                                                            "res_col_id_to," +
                                                            "parse_time," +
                                                            "exec_sql_analyzer_duration," +
                                                            "exec_sql_str_example," +
                                                            "exec_sql_analyzer_user," +
                                                            "date_id," +
                                                            "script_path ) values (?,?,?,?,?,?,?,?)";

                                                    try {
                                                        System.out.println(queryText1);
                                                        PreparedStatement preparedStatement = conn.prepareStatement(sql);
                                                        preparedStatement.setString(1, source);
                                                        preparedStatement.setString(2, target);
                                                        preparedStatement.setTimestamp(3, new Timestamp(start_time * 1_000));
                                                        preparedStatement.setLong(4, duration);
                                                        preparedStatement.setString(5, queryText1);
                                                        preparedStatement.setString(6, "");
                                                        preparedStatement.setInt(7, Integer.valueOf(now));
                                                        preparedStatement.setString(8, "");
                                                        preparedStatement.executeUpdate();
                                                        preparedStatement.close();
                                                    } catch (Exception e) {
                                                        e.printStackTrace();
                                                    }


                                                }

                                            }
                                        });


                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        IOUtils.closeStream(bufferReader);
                    }


                }
        );


    }
}
