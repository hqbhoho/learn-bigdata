 /* ***************************************** SQL Begin ***************************************** */
 /* 脚本功能: 销户客户指标统计                                                                    */
 /* 脚本归属:DSC_STAT                                                                             */ 
 /* 修改记录:                                                                                     */
 /* ----------------------------------------------------------------------------------------------*/ 
 /*          操作人员    操作日期    操作说明                                                     */
 /*          李伟        20180108    创建                                                         */
 /* ********************************************************************************************* */    
    ----定义变量
    --销户客户指标回溯日期类型
    SET VAR:VN_XHKH_HSRQ=UDFS.F_GET_PARAMVALUE('01','XHKH_HSRQ','',1);
    --销户客户指标回溯偏移类型
    SET VAR:VN_N_DATE_PYTYPE=SUBSTR(${VAR:VN_XHKH_HSRQ},1,INSTR(${VAR:VN_XHKH_HSRQ}, ':') - 1);
    --销户客户指标回溯的偏移量
    SET VAR:VN_N_DATE_PYZ =SUBSTR(${VAR:VN_XHKH_HSRQ},INSTR(${VAR:VN_XHKH_HSRQ}, ':') +1);
    --获取非交易日销户日期
    SET VAR:VN_DATE_PRE = UDFS.F_GET_DATE_PY(UDFS.F_GET_JYR_DATE(${VAR:RQ},-1),'R',1,'YYYYMMDD');
  
    --STEP 0：删除临时表
    DROP TABLE IF EXISTS TEMPTEST.TMP_XHKH_HSRQ;
    DROP TABLE IF EXISTS TEMPTEST.TMP_COMMON_XHKH;
    DROP TABLE IF EXISTS TEMPTEST.TMP_T_STAT_XHKH_01;
	DROP TABLE IF EXISTS TEMPTEST.TMP_T_STAT_XHKH_BAK;
    
    --STEP 1:创建临时表
	--备份数据统计日期之前的销户客户数据
	CREATE TABLE TEMPTEST.TMP_T_STAT_XHKH_BAK
	AS
	 SELECT * FROM DSC_STAT.T_STAT_XHKH T
		 WHERE NOT EXISTS (SELECT 1
				  FROM DSC_BAS.T_KHXX_JJYW T1
				 WHERE T.KHH = T1.KHH
				   AND T1.KHZT = '3' AND T1.XHRQ BETWEEN ${VAR:VN_DATE_PRE} AND ${VAR:RQ});
	
    CREATE TABLE TEMPTEST.TMP_XHKH_HSRQ(PYTYPE STRING,PYZ INT ,XHKH_HSRQ INT);
    
    --1.1 插入客户销户的追溯规则数据
    INSERT INTO TEMPTEST.TMP_XHKH_HSRQ(PYTYPE ,PYZ )
     VALUES (${VAR:VN_N_DATE_PYTYPE},CAST(${VAR:VN_N_DATE_PYZ} AS INT)) ;
   
    --1.2 更新追溯日期
    INSERT OVERWRITE TEMPTEST.TMP_XHKH_HSRQ
    (PYTYPE, PYZ, XHKH_HSRQ)
    SELECT PYTYPE,
           PYZ,
           CASE
               WHEN PYTYPE = 'JYR' THEN
                UDFS.F_GET_JYR_DATE(${VAR:RQ}, CAST(-PYZ AS INT)) --前N个交易日
               WHEN PYTYPE = 'ZRR' THEN
                UDFS.F_GET_DATE_PY(${VAR:RQ}, 'R', CAST(-PYZ AS INT), 'YYYYMMDD') --前N个自然日
               WHEN PYTYPE = 'Y' THEN
                UDFS.F_GET_DATE_PY(${VAR:RQ}, 'Y', CAST(-PYZ AS INT), 'YYYYMMDD') --前N个月
               WHEN PYTYPE = 'N' THEN
                UDFS.F_GET_DATE_PY(${VAR:RQ}, 'N', CAST(-PYZ AS INT), 'YYYYMMDD') --前N年
           END AS XHKH_HSRQ
      FROM TEMPTEST.TMP_XHKH_HSRQ;
    
    --STEP 2:创建临时表
    CREATE TABLE TEMPTEST.TMP_COMMON_XHKH
    AS
    SELECT T.KHH,
               T.YYB,
               T.GRJG,
               T.KHRQ,
               T.XHRQ,
               0 AS TS, --销户前一个月交易日的交易天数
               D.ZJJYR,
               D.SSCJYRQ
          FROM DSC_BAS.T_KHXX_JJYW T LEFT JOIN 
               (SELECT KHH, YYB, ZJJYR, SSCJYRQ
                  FROM DSC_STAT.T_STAT_ZJJYR
                 WHERE ZHLB = '1' --取普通账户的
                ) D
         ON T.KHH = D.KHH
           WHERE   1=2;
    
    --STEP 2.1 将销户客户生成到临时表[TEMPTEST.TMP_COMMON_XHKH]
    INSERT INTO TEMPTEST.TMP_COMMON_XHKH
        (KHH, YYB, GRJG, KHRQ, XHRQ, TS)
        SELECT T.KHH,
               T.YYB,
               T.GRJG,
               T.KHRQ,
               T.XHRQ,
               CAST(COUNT(DISTINCT A.JYR) AS TINYINT) AS TS --销户前一个月交易日的交易天数
          FROM DSC_BAS.T_KHXX_JJYW T
          LEFT JOIN (SELECT T1.JYR
                       FROM DSC_CFG.T_XTJYR T1,
                            TEMPTEST.TMP_XHKH_HSRQ T2
                      WHERE T1.JYR >= T2.XHKH_HSRQ) A
            ON A.JYR <= T.XHRQ
           AND A.JYR BETWEEN T.KHRQ AND T.XHRQ
         WHERE T.KHZT = '3'
           AND T.XHRQ BETWEEN ${VAR:VN_DATE_PRE} AND ${VAR:RQ}
         GROUP BY T.KHH, T.YYB, T.GRJG, T.KHRQ, T.XHRQ;
         
     --STEP 2.2:更新销户客户的最近交易日和上上次交易日
     INSERT OVERWRITE TEMPTEST.TMP_COMMON_XHKH
            (KHH, YYB, GRJG, KHRQ, XHRQ, TS, ZJJYR, SSCJYRQ)
            SELECT T.KHH, T.YYB, T.GRJG, T.KHRQ, T.XHRQ, T.TS, D.ZJJYR, D.SSCJYRQ
              FROM  TEMPTEST.TMP_COMMON_XHKH T LEFT JOIN
               (SELECT KHH, YYB, ZJJYR, SSCJYRQ
                  FROM DSC_STAT.T_STAT_ZJJYR
                 WHERE ZHLB = '1' --取普通账户的
                ) D
          ON T.KHH = D.KHH;
             
     --STEP 3:创建临时表
     CREATE TABLE TEMPTEST.TMP_T_STAT_XHKH_01
     AS
     SELECT * FROM DSC_STAT.T_STAT_XHKH
     WHERE 1 = 2;
      
      
     --STEP 4:将客户资产数据(普通账户)插入到临时表[TEMPTEST.TEMPTEST.TMP_T_STAT_XHKH_01]
     INSERT INTO TEMPTEST.TMP_T_STAT_XHKH_01
          (KHH,
           YYB,
           GRJG,
           XHRQ,
           CRJE_RMB,
           CRJE_HKD,
           CRJE_USD,
           QCJE_RMB,
           QCJE_HKD,
           QCJE_USD,
           JLCZJ,
           ZRZQSZ,
           ZD_RMB,
           ZD_USD,
           ZTGZR_RMB,
           ZTGZR_HKD,
           ZCZQSZ,
           CZD_RMB,
           CZD_USD,
           ZTGZC_RMB,
           ZTGZC_HKD,
           JLCZQSZ,
           RJZC, --临时表保存是累计值，后面做除法处理
           RJZJYE, --临时表保存是累计值，后面做除法处理
           RJZQSZ --临时表保存是累计值，后面做除法处理
           )
          SELECT KHH,
                 '1' AS YYB, --临时表不允许为空，就给个1，反正后面不会用到
                 1 AS GRJG, --临时表不允许为空，就给个1，反正后面不会用到
                 0 AS XHRQ, --临时表不允许为空，就给个0，反正后面不会用到
                 CAST(SUM(CRJE_RMB) AS  DECIMAL(16,2)) AS CRJE_RMB,
                 CAST(SUM(CRJE_HKD) AS  DECIMAL(16,2)) AS CRJE_HKD,
                 CAST(SUM(CRJE_USD) AS  DECIMAL(16,2)) AS CRJE_USD,
                 CAST(SUM(QCJE_RMB) AS  DECIMAL(16,2)) AS QCJE_RMB,
                 CAST(SUM(QCJE_HKD) AS  DECIMAL(16,2)) AS QCJE_HKD,
                 CAST(SUM(QCJE_USD) AS  DECIMAL(16,2)) AS QCJE_USD,
                 CAST(SUM(CRJE_RMB + CRJE_HKD * HLCS_HKD + CRJE_USD * HLCS_USD) -
                 SUM(QCJE_RMB + QCJE_HKD * HLCS_HKD + QCJE_USD * HLCS_USD) AS  DECIMAL(16,2)) AS JLCZJ,
                 CAST(SUM(ZRZQSZ) AS DECIMAL(22,2)) AS ZRZQSZ,
                 CAST(SUM(ZD_RMB) AS DECIMAL(16,2)) AS ZD_RMB,
                 CAST(SUM(ZD_USD) AS DECIMAL(16,2))  AS ZD_USD,
                 CAST(SUM(ZTGZR_RMB) AS DECIMAL(16,2)) AS ZTGZR_RMB,
                 CAST(SUM(ZTGZR_HKD) AS DECIMAL(16,2)) AS ZTGZR_HKD,
                 CAST(SUM(ZCZQSZ) AS DECIMAL(22,2))  AS ZCZQSZ,
                 CAST(SUM(CZD_RMB) AS DECIMAL(16,2)) AS CZD_RMB,
                 CAST(SUM(CZD_USD) AS DECIMAL(16,2)) AS CZD_USD,
                 CAST(SUM(ZTGZC_RMB) AS DECIMAL(16,2)) AS ZTGZC_RMB,
                 CAST(SUM(ZTGZC_HKD) AS DECIMAL(16,2)) AS ZTGZC_HKD,
                 CAST(SUM(ZRZQSZ - ZCZQSZ) AS DECIMAL(16,2)) AS JLCZQSZ,
                 CAST(CASE
                     WHEN COUNT(KHH) = 0 THEN
                      0
                     ELSE
                      SUM(ZZC) / COUNT(KHH)
                 END AS DECIMAL(16,2)) AS  RJZZC,
                 CAST(CASE
                     WHEN COUNT(KHH) = 0 THEN
                      0
                     ELSE
                      SUM(ZJYE_RMB + ZJYE_HKD * HLCS_HKD + ZJYE_USD * HLCS_USD) /
                      COUNT(KHH)
                 END AS DECIMAL(16,2)) AS  RJZJYE,
                 CAST(CASE
                     WHEN COUNT(KHH) = 0 THEN
                      0
                     ELSE
                      SUM(ZQSZ_RMB + ZQSZ_HKD * HLCS_HKD + ZQSZ_USD * HLCS_USD) /
                      COUNT(KHH)
                 END AS DECIMAL(22,2)) AS RJZQSZ
            FROM DSC_STAT.T_STAT_KHZC_R T, TEMPTEST.TMP_XHKH_HSRQ  A
           WHERE T.RQ >=A.XHKH_HSRQ AND T.RQ <= ${VAR:RQ}
             AND EXISTS (SELECT 1 FROM TEMPTEST.TMP_COMMON_XHKH D WHERE T.KHH = D.KHH)
           GROUP BY KHH;
           
    --STEP 5:将客户资产数据(信用账户)插入到临时表[TEMPTEST.TMP_T_STAT_XHKH_01]
    INSERT INTO TEMPTEST.TMP_T_STAT_XHKH_01
          (KHH,
           YYB,
           GRJG,
           XHRQ,
           CRJE_RMB,
           QCJE_RMB,
           JLCZJ,
           ZRZQSZ,
           ZCZQSZ,
           JLCZQSZ,
           RJZC, --临时表保存是累计值，后面做除法处理
           RJZJYE, --临时表保存是累计值，后面做除法处理
           RJZQSZ --临时表保存是累计值，后面做除法处理
           )
          SELECT  KHH,
                 '1' AS YYB, --临时表不允许为空，就给个1，反正后面不会用到
                 1 AS GRJG, --临时表不允许为空，就给个1，反正后面不会用到
                 0 AS XHRQ, --临时表不允许为空，就给个0，反正后面不会用到
                 CAST(SUM(ZRJE) AS DECIMAL(16,2)) AS CRJE_RMB,
                 CAST(SUM(ZCJE) AS DECIMAL(16,2)) AS QCJE_RMB,
                 CAST(SUM(ZRJE) - SUM(ZCJE) AS DECIMAL(16,2)) AS JLCZJ,
                 CAST(-SUM(T.ZCSZ) AS  DECIMAL(22,2)) AS ZRZQSZ, --对于总体账户而言，普通账户的转入包含了从信用账户转过来的部分，所以这里要扣除掉账户间的转移，只考虑外部转入
                 CAST(-SUM(T.ZRSZ) AS  DECIMAL(22,2)) AS ZCZQSZ, --对于总体账户而言，普通账户的转出包含了转给信用账户的部分，所以这里要扣除掉账户间的转移，只考虑外部转入
                 CAST(SUM(T.ZRSZ - T.ZCSZ) AS  DECIMAL(22,2))  AS JLCZQSZ,
                 CAST(CASE
                     WHEN COUNT(KHH) = 0 THEN
                      0
                     ELSE
                      SUM(ZZC) / COUNT(KHH)
                 END AS DECIMAL(16,2)) AS RJZZC,
                 CAST(CASE
                     WHEN COUNT(KHH) = 0 THEN
                      0
                     ELSE
                      SUM(ZJYE) / COUNT(KHH)
                 END AS DECIMAL(16,2)) AS  RJZJYE,
                 CAST(CASE
                     WHEN COUNT(KHH) = 0 THEN
                      0
                     ELSE
                      SUM(ZQSZ) / COUNT(KHH)
                 END  AS DECIMAL(22,2)) RJZQSZ
            FROM DSC_STAT.T_STAT_RZRQ_R T ,TEMPTEST.TMP_XHKH_HSRQ A
           WHERE T.RQ >= A.XHKH_HSRQ AND T.RQ <= ${VAR:RQ}
             AND EXISTS (SELECT 1 FROM TEMPTEST.TMP_COMMON_XHKH D WHERE T.KHH = D.KHH)
           GROUP BY KHH;
           
    --STEP 6:将客户交易收入（普通账户+信用账户）数据插入到临时表[TEMPTEST.TMP_T_STAT_XHKH_01]
    INSERT INTO TEMPTEST.TMP_T_STAT_XHKH_01
          (KHH,
           YYB,
           GRJG,
           XHRQ,
           JLCZQSZ,
           JYL_RMB,
           JYL_HKD,
           JYL_USD,
           YJSR_RMB,
           YJSR_HKD,
           YJSR_USD,
           JYJ_RMB,
           JYJ_HKD,
           JYJ_USD,
           YJL,
           JYL_GJ)
          SELECT KHH,
                 '1' AS YYB, --临时表不允许为空，就给个1，反正后面不会用到
                 1 AS GRJG, --临时表不允许为空，就给个1，反正后面不会用到
                 0 AS XHRQ, --临时表不允许为空，就给个0，反正后面不会用到
                 0 AS JLCZQSZ, --临时表不允许为空，就给个0
                 CAST(SUM(JYL_RMB) AS DECIMAL(16, 2)) AS JYL_RMB,
                 CAST(SUM(JYL_HKD) AS DECIMAL(16, 2)) AS JYL_HKD,
                 CAST(SUM(JYL_USD) AS DECIMAL(16, 2)) AS JYL_USD,
                 CAST(SUM(YJSR_RMB) AS DECIMAL(16, 2)) AS YJSR_RMB,
                 CAST(SUM(YJSR_HKD) AS DECIMAL(16, 2)) AS YJSR_HKD,
                 CAST(SUM(YJSR_USD) AS DECIMAL(16, 2)) AS YJSR_USD,
                 CAST(SUM(JYJ_RMB) AS DECIMAL(16, 2)) AS JYJ_RMB,
                 CAST(SUM(JYJ_HKD) AS DECIMAL(16, 2)) AS JYJ_HKD,
                 CAST(SUM(JYJ_USD) AS DECIMAL(16, 2)) AS JYJ_USD,
                 CAST(CASE
                          WHEN SUM(ZJYL) = 0 THEN
                           0
                          ELSE
                           SUM(ZJYJ) / SUM(ZJYL) * 1000
                      END AS DECIMAL(16, 2)) AS YJL,
                 CAST(SUM(JYL_GP) + SUM(JYL_JJ) AS DECIMAL(16, 2)) AS JYL_GJ
            FROM (SELECT KHH,
                         SUM(JYL_HAZB + JYL_SAZB + JYL_ZXB + JYL_CYB + JYL_FBSJJ + JYL_ETF +
                             JYL_LOF + JYL_SB * HLCS_HKD + JYL_HB * HLCS_USD) AS ZJYL,
                         SUM(JYL_HAZB + JYL_SAZB + JYL_ZXB + JYL_CYB + JYL_FBSJJ + JYL_ETF +
                             JYL_LOF) AS JYL_RMB,
                         SUM(JYL_SB) AS JYL_HKD,
                         SUM(JYL_HB) AS JYL_USD,
                         SUM(JYL_HAZB + JYL_SAZB + JYL_ZXB + JYL_CYB + JYL_HB * HLCS_USD +
                             JYL_SB * HLCS_HKD + JYL_SB_A + JYL_SB_B) JYL_GP,
                         SUM(JYL_FBSJJ + JYL_ETF + JYL_LOF + JYL_SZJJT + JYL_DXJJ) JYL_JJ,
                         SUM(JYL_QZ) JYL_QZ,
                         SUM(JYL_GZ + JYL_GSQYZ + JYL_KZZ) JYL_ZQ,
                         SUM(JYL_QT) JYL_QT,
                         SUM(YJSR_HAZB + YJSR_SAZB + YJSR_ZXB + YJSR_CYB + YJSR_FBSJJ +
                             YJSR_ETF + YJSR_LOF) AS YJSR_RMB,
                         SUM(YJSR_SB) AS YJSR_HKD,
                         SUM(YJSR_HB) AS YJSR_USD,
                         SUM(JYJ_HAZB + JYJ_SAZB + JYJ_ZXB + JYJ_CYB + JYJ_FBSJJ + JYJ_ETF +
                             JYJ_LOF + JYJ_SB * HLCS_HKD + JYJ_HB * HLCS_USD) AS ZJYJ,
                         SUM(JYJ_HAZB + JYJ_SAZB + JYJ_ZXB + JYJ_CYB + JYJ_FBSJJ + JYJ_ETF +
                             JYJ_LOF) AS JYJ_RMB,
                         SUM(JYJ_SB) AS JYJ_HKD,
                         SUM(JYJ_HB) AS JYJ_USD
                    FROM DSC_STAT.T_STAT_KHJYSR_R T ,TEMPTEST.TMP_XHKH_HSRQ  A
                   WHERE T.RQ >= A.XHKH_HSRQ  AND T.RQ <= ${VAR:RQ}
                     AND EXISTS
                   (SELECT 1 FROM TEMPTEST.TMP_COMMON_XHKH D WHERE T.KHH = D.KHH)
                   GROUP BY KHH
                  UNION ALL
                  SELECT KHH,
                         SUM(JYL) AS ZJYL,
                         SUM(JYL) AS JYL_RMB,
                         0 AS JYL_HKD,
                         0 AS JYL_USD,
                         0 AS JYL_GP,
                         0 AS JYL_JJ,
                         0 AS JYL_QZ,
                         0 AS JYL_ZQ,
                         0 AS JYL_QT,
                         SUM(YJ) AS YJSR_RMB,
                         0 AS YJSR_HKD,
                         0 AS YJSR_USD,
                         SUM(JYJ) AS ZJYJ,
                         SUM(JYJ) AS JYJ_RMB,
                         0 AS JYJ_HKD,
                         0 AS JYJ_USD
                    FROM DSC_STAT.T_STAT_RZRQ_R T ,TEMPTEST.TMP_XHKH_HSRQ A
                   WHERE T.RQ >= A.XHKH_HSRQ  AND T.RQ <= ${VAR:RQ}
                     AND EXISTS
                   (SELECT 1 FROM TEMPTEST.TMP_COMMON_XHKH D WHERE T.KHH = D.KHH)
                   GROUP BY KHH) T
           GROUP BY T.KHH;
       
         
    --STEP 7：创建临时表
    CREATE TABLE TEMPTEST.TMP_T_STAT_XHKH_02
    AS
    SELECT * FROM DSC_STAT.T_STAT_XHKH
    WHERE 1 = 2;
     
    --插入新增的销户客户统计数据以及现有的销户客户数据过滤之前客户销户后又做了取消销户动作的数据，这些客户的销户日期安按最新的来统计
    INSERT INTO TEMPTEST.TMP_T_STAT_XHKH_02
          (KHH,
           YYB,
           GRJG,
           JYL_RMB,
           JYL_HKD,
           JYL_USD,
           YJSR_RMB,
           YJSR_HKD,
           YJSR_USD,
           JYJ_RMB,
           JYJ_HKD,
           JYJ_USD,
           YJL,
           RJZC,
           CRJE_RMB,
           CRJE_HKD,
           CRJE_USD,
           QCJE_RMB,
           QCJE_HKD,
           QCJE_USD,
           JLCZJ,
           ZRZQSZ,
           ZD_RMB,
           ZD_USD,
           ZTGZR_RMB,
           ZTGZR_HKD,
           ZCZQSZ,
           CZD_RMB,
           CZD_USD,
           ZTGZC_RMB,
           ZTGZC_HKD,
           JLCZQSZ,
           XHRQ,
           ZJJYR,
           RJZQSZ,
           RJZJYE,
           JYL_GJ)
          SELECT T1.KHH,
                 T1.YYB,
                 T1.GRJG,
                 CAST(NVL(JYL_RMB, 0) AS DECIMAL(16,2)),
                 CAST(NVL(JYL_HKD, 0) AS DECIMAL(16,2)) ,
                 CAST(NVL(JYL_USD, 0) AS DECIMAL(16,2)) ,
                 CAST(NVL(YJSR_RMB, 0) AS DECIMAL(16,2)) ,
                 CAST(NVL(YJSR_HKD, 0) AS DECIMAL(16,2)) ,
                 CAST(NVL(YJSR_USD, 0) AS DECIMAL(16,2)) ,
                 CAST(NVL(JYJ_RMB, 0) AS DECIMAL(16,2)) ,
                 CAST(NVL(JYJ_HKD, 0) AS DECIMAL(16,2)) ,
                 CAST(NVL(JYJ_USD, 0) AS DECIMAL(16,2)) ,
                 CAST(NVL(YJL, 0) AS DECIMAL(16,2))  AS YJL,
                 CAST(NVL(RJZC, 0) AS DECIMAL(16,2)) AS RJZC,
                 CAST(NVL(T2.CRJE_RMB, 0) AS DECIMAL(16,2)) ,
                 CAST(NVL(T2.CRJE_HKD, 0) AS DECIMAL(16,2)) ,
                 CAST(NVL(T2.CRJE_USD, 0) AS DECIMAL(16,2)) ,
                 CAST(NVL(T2.QCJE_RMB, 0) AS DECIMAL(16,2)) ,
                 CAST(NVL(T2.QCJE_HKD, 0) AS DECIMAL(16,2)) ,
                 CAST(NVL(T2.QCJE_USD, 0) AS DECIMAL(16,2)) ,
                 CAST(NVL(T2.JLCZJ, 0) AS DECIMAL(16,2)) AS JLCZJ,
                 CAST(NVL(T2.ZRZQSZ, 0) AS DECIMAL(22,2)),
                 CAST(NVL(T2.ZD_RMB, 0) AS DECIMAL(16,2)) ,
                 CAST(NVL(T2.ZD_USD, 0) AS DECIMAL(16,2)),
                 CAST(NVL(T2.ZTGZR_RMB, 0) AS DECIMAL(16,2)),
                 CAST(NVL(T2.ZTGZR_HKD, 0) AS DECIMAL(16,2)),
                 CAST(NVL(T2.ZCZQSZ, 0) AS DECIMAL(22,2)),
                 CAST(NVL(T2.CZD_RMB, 0) AS DECIMAL(16,2)) ,
                 CAST(NVL(T2.CZD_USD, 0) AS DECIMAL(16,2)) ,
                 CAST(NVL(T2.ZTGZC_RMB, 0) AS DECIMAL(16,2)) ,
                 CAST(NVL(T2.ZTGZC_HKD, 0) AS DECIMAL(16,2)),
                 CAST(NVL(T2.JLCZQSZ, 0) AS DECIMAL(22,2)),
                 T1.XHRQ,
                 T1.ZJJYR,
                 CAST(NVL(T2.RJZQSZ, 0) AS DECIMAL(22,2)),
                 CAST(NVL(T2.RJZJYE, 0) AS DECIMAL(16,2)) ,
                 CAST(NVL(T2.JYL_GJ, 0) AS DECIMAL(16,2)) AS JYL_GJ
            FROM (SELECT KHH, YYB, GRJG, KHRQ, XHRQ, TS, ZJJYR FROM TEMPTEST.TMP_COMMON_XHKH) T1
            LEFT JOIN (SELECT KHH,
                              SUM(JYL_RMB) AS JYL_RMB,
                              SUM(JYL_HKD) AS JYL_HKD,
                              SUM(JYL_USD) AS JYL_USD,
                              SUM(YJSR_RMB) AS YJSR_RMB,
                              SUM(YJSR_HKD) AS YJSR_HKD,
                              SUM(YJSR_USD) AS YJSR_USD,
                              SUM(JYJ_RMB) AS JYJ_RMB,
                              SUM(JYJ_HKD) AS JYJ_HKD,
                              SUM(JYJ_USD) AS JYJ_USD,
                              SUM(YJL) AS YJL,
                              SUM(RJZC) AS RJZC,
                              SUM(CRJE_RMB) AS CRJE_RMB,
                              SUM(CRJE_HKD) AS CRJE_HKD,
                              SUM(CRJE_USD) AS CRJE_USD,
                              SUM(QCJE_RMB) AS QCJE_RMB,
                              SUM(QCJE_HKD) AS QCJE_HKD,
                              SUM(QCJE_USD) AS QCJE_USD,
                              SUM(JLCZJ) AS JLCZJ,
                              SUM(ZRZQSZ) AS ZRZQSZ,
                              SUM(ZD_RMB) AS ZD_RMB,
                              SUM(ZD_USD) AS ZD_USD,
                              SUM(ZTGZR_RMB) AS ZTGZR_RMB,
                              SUM(ZTGZR_HKD) AS ZTGZR_HKD,
                              SUM(ZCZQSZ) AS ZCZQSZ,
                              SUM(CZD_RMB) AS CZD_RMB,
                              SUM(CZD_USD) AS CZD_USD,
                              SUM(ZTGZC_RMB) AS ZTGZC_RMB,
                              SUM(ZTGZC_HKD) AS ZTGZC_HKD,
                              SUM(JLCZQSZ) AS JLCZQSZ,
                              SUM(XHRQ) AS XHRQ,
                              SUM(ZJJYR) AS ZJJYR,
                              SUM(RJZQSZ) AS RJZQSZ,
                              SUM(RJZJYE) AS RJZJYE,
                              SUM(JYL_GJ) AS JYL_GJ
                         FROM TEMPTEST.TMP_T_STAT_XHKH_01
                        GROUP BY KHH) T2
              ON T1.KHH = T2.KHH
          UNION ALL
          SELECT KHH,
                 YYB,
                 GRJG,
                 JYL_RMB,
                 JYL_HKD,
                 JYL_USD,
                 YJSR_RMB,
                 YJSR_HKD,
                 YJSR_USD,
                 JYJ_RMB,
                 JYJ_HKD,
                 JYJ_USD,
                 YJL,
                 RJZC,
                 CRJE_RMB,
                 CRJE_HKD,
                 CRJE_USD,
                 QCJE_RMB,
                 QCJE_HKD,
                 QCJE_USD,
                 JLCZJ,
                 ZRZQSZ,
                 ZD_RMB,
                 ZD_USD,
                 ZTGZR_RMB,
                 ZTGZR_HKD,
                 ZCZQSZ,
                 CZD_RMB,
                 CZD_USD,
                 ZTGZC_RMB,
                 ZTGZC_HKD,
                 JLCZQSZ,
                 XHRQ,
                 ZJJYR,
                 RJZQSZ,
                 RJZJYE,
                 JYL_GJ
            FROM DSC_STAT.T_STAT_XHKH T
           WHERE NOT EXISTS (SELECT 1
                    FROM TEMPTEST.TMP_COMMON_XHKH T1);   --过滤之前客户销户后又做了取消销户动作的数据，这些客户的销户日期安按最新的来统计
           
     
    --SETP 8：将临时表中的数据插入到目标表
	INSERT OVERWRITE DSC_STAT.T_STAT_XHKH
		(KHH,
		 YYB,
		 GRJG,
		 JYL_RMB,
		 JYL_HKD,
		 JYL_USD,
		 YJSR_RMB,
		 YJSR_HKD,
		 YJSR_USD,
		 JYJ_RMB,
		 JYJ_HKD,
		 JYJ_USD,
		 YJL,
		 RJZC,
		 CRJE_RMB,
		 CRJE_HKD,
		 CRJE_USD,
		 QCJE_RMB,
		 QCJE_HKD,
		 QCJE_USD,
		 JLCZJ,
		 ZRZQSZ,
		 ZD_RMB,
		 ZD_USD,
		 ZTGZR_RMB,
		 ZTGZR_HKD,
		 ZCZQSZ,
		 CZD_RMB,
		 CZD_USD,
		 ZTGZC_RMB,
		 ZTGZC_HKD,
		 JLCZQSZ,
		 XHRQ,
		 ZJJYR,
		 RJZQSZ,
		 RJZJYE,
		 JYL_GJ)
		SELECT KHH,
			   YYB,
			   GRJG,
			   JYL_RMB,
			   JYL_HKD,
			   JYL_USD,
			   YJSR_RMB,
			   YJSR_HKD,
			   YJSR_USD,
			   JYJ_RMB,
			   JYJ_HKD,
			   JYJ_USD,
			   YJL,
			   RJZC,
			   CRJE_RMB,
			   CRJE_HKD,
			   CRJE_USD,
			   QCJE_RMB,
			   QCJE_HKD,
			   QCJE_USD,
			   JLCZJ,
			   ZRZQSZ,
			   ZD_RMB,
			   ZD_USD,
			   ZTGZR_RMB,
			   ZTGZR_HKD,
			   ZCZQSZ,
			   CZD_RMB,
			   CZD_USD,
			   ZTGZC_RMB,
			   ZTGZC_HKD,
			   JLCZQSZ,
			   XHRQ,
			   ZJJYR,
			   RJZQSZ,
			   RJZJYE,
			   JYL_GJ
		  FROM TEMPTEST.TMP_T_STAT_XHKH_02
		UNION ALL
		SELECT KHH,
			   YYB,
			   GRJG,
			   JYL_RMB,
			   JYL_HKD,
			   JYL_USD,
			   YJSR_RMB,
			   YJSR_HKD,
			   YJSR_USD,
			   JYJ_RMB,
			   JYJ_HKD,
			   JYJ_USD,
			   YJL,
			   RJZC,
			   CRJE_RMB,
			   CRJE_HKD,
			   CRJE_USD,
			   QCJE_RMB,
			   QCJE_HKD,
			   QCJE_USD,
			   JLCZJ,
			   ZRZQSZ,
			   ZD_RMB,
			   ZD_USD,
			   ZTGZR_RMB,
			   ZTGZR_HKD,
			   ZCZQSZ,
			   CZD_RMB,
			   CZD_USD,
			   ZTGZC_RMB,
			   ZTGZC_HKD,
			   JLCZQSZ,
			   XHRQ,
			   ZJJYR,
			   RJZQSZ,
			   RJZJYE,
			   JYL_GJ
		  FROM TEMPTEST.TMP_T_STAT_XHKH_BAK;
            
    --STEP 99：删除临时表
    DROP TABLE IF EXISTS TEMPTEST.TMP_XHKH_HSRQ;
    DROP TABLE IF EXISTS TEMPTEST.TMP_COMMON_XHKH;
    DROP TABLE IF EXISTS TEMPTEST.TMP_T_STAT_XHKH_01;
    DROP TABLE IF EXISTS TEMPTEST.TMP_T_STAT_XHKH_02;
	DROP TABLE IF EXISTS TEMPTEST.TMP_T_STAT_XHKH_BAK;