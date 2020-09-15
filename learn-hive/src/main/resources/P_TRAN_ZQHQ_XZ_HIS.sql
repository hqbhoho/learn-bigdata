/*
	@@name:修正证券行情清洗转换程序
	@@componet_type: impala
	@@author:陈德铸
	@@version: V1.0.1
    @@update_time:2019-07-24
	@@comment:EMC_APEX 修改：源表从SRC_ABOSS调整为从dsc_bas和dsc_cfg库下的表进行修正
	@@注意事项：1.该脚本依赖于DSC_CFG.T_ZQDM、DSC_BAS.T_ZQHQ_HIS以及DSC_CFG.T_HLCS_EXT表
	            2.证券代码表获取ZXLZ字段，导致数仓该表也要有历史数据，补历史数据，证券代码清洗也要调度
    修改记录
    ------------------------------------------------
    李伟     20190820      修改：可转债申购代码 申购日期至中签公布日行情修正为0
	李伟     20190903      修改：1、分级基金行情直接使用info.this_jjjz（分级基金历史净值表）
                                 2、上海可转债认购代码的行情从申购日T+1天才有，对申购日(包含申购日)之前的行情修正为0
	李伟     20190904      修改：调整配股的行情：1、除权除息日前一天，正股行情进行修正，配股行情修正为与正股价格一致。
	                             2、配股行情修正为与正股价格一致，这个操作直到上市前一日，然后消除配股行情
    李伟     20190911      修改：通过SRC_ABOSS.TZQDM更新xzlx字段,支持历史数据重跑
	李伟     20191018      修改：新股管理中，ssrq=0的数据需要赋默认值，避免新股行情修正不到
	李伟     20191025      修改：1、深圳可转债申购旧规：债券代码的行情从中签公布日+1一直修复到上市日期-1
	                             2、可转债申购代码行情新规：从20170901日开始申购日期至中签公布日行情修正为0
	张伟真	 20191117	   修改：上海ETF行情修正0置1，并补充E3认购代码行情
	周在键	 20191224	   修改： 行情表与INFO.TGP_GSFHKG关联时，去掉INFO.TGP_GSFHKG中cqcxrq为null的数据。
	周在键   20200307      修改：港股的行情不修正，避免计算市值的时候，因为精度问题导致港股市值差异大	
	周在键   20200513      修改：处理同一支因多次配股引起的，配股行情丢失问题。
*/

-----初始化参数变量-----
--上一个交易日
SET VAR:JYR_PRE1=UDFS.F_GET_JYR_DATE(${VAR:RQ}, -1);
--下一个交易日
SET VAR:JYR_AFT1=UDFS.F_GET_JYR_DATE(${VAR:RQ}, 1);

-----STEP 0:删除临时表
DROP TABLE IF EXISTS TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_1;
DROP TABLE IF EXISTS TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_2;
DROP TABLE IF EXISTS TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_3;
DROP TABLE IF EXISTS TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_3_1;
DROP TABLE IF EXISTS TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_4;
DROP TABLE IF EXISTS TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_5;
DROP TABLE IF EXISTS TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_6;


-----STEP 1:根据股票权益类数据生成中间表----
CREATE TABLE  TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_1
 AS
SELECT T.RQ,
       T.JYS,
       T.ZQDM,
       D.ZQMC,
       T.JYDW,
       CASE
           WHEN T.JYS = FHKG.JYS AND T.ZQDM = FHKG.ZQDM THEN
            ROUND((T.ZXJ - FHKG.SQPXBL) / (1 + CASE   --税前派现比例
                      WHEN T.JYS = '1' THEN --深圳
                       FHKG.SGBL + FHKG.ZZBL    --送股比例+转增比例
                      ELSE
                       0
                  END),
                  4)
           WHEN T.JYS = PG.JYS AND T.ZQDM = PG.ZQDM THEN
            ROUND((T.ZXJ * 1 + PG.PGJG * PG.PGBL) / (1 + PG.PGBL), 4)
		   WHEN T.JYS = '2' AND (T.ZQDM LIKE '5%' OR D.ZQLB LIKE 'E%') AND T.ZXJ = 0 THEN
		    1
           ELSE
            T.ZXJ
       END ZXJ,
       T.ZSP,
       T.JKP,
       T.ZGJ,
       T.ZDJ,
       T.CJSL,
       T.CJJE,
       T.ZXLX,
       T.ZXJ ZXJ_ORG,
       T.ZSP ZSP_ORG,
       CASE
           WHEN FHKG.SQPXBL > 0 AND FHKG.SGBL + FHKG.ZZBL > 0 THEN
            7
           WHEN FHKG.SQPXBL > 0 THEN
            5
           WHEN FHKG.SGBL + FHKG.ZZBL > 0 THEN
            6
           WHEN T.JYS = PG.JYS AND T.ZQDM = PG.ZQDM THEN
            8
           ELSE
            1
       END GZJ_FLAG
  FROM DSC_BAS.T_ZQHQ_HIS T
  LEFT JOIN DSC_CFG.T_ZQDM D
    ON T.JYS = D.JYS
   AND T.ZQDM = D.ZQDM
  LEFT JOIN (SELECT JYS, ZQDM, SGBL, ZZBL, SQPXBL
               FROM INFO.TGP_GSFHKG --股票-分红扩股信息
              WHERE GQDJRQ = ${VAR:RQ} AND CQCXRQ IS NOT NULL
                AND CQCXRQ <> 0) FHKG -- 股权登记日期  --20191224调整
    ON T.JYS = FHKG.JYS
   AND T.ZQDM = FHKG.ZQDM
  LEFT JOIN (SELECT JYS, ZQDM, PGBL, PGJG
               FROM INFO.TGP_GPPG --股票-股票配股信息
              WHERE CQCXRQ IS NOT NULL
                AND CQCXRQ <> 0
                AND UDFS.F_GET_JYR_DATE(CQCXRQ, -1) = ${VAR:RQ}) PG -- 配股  (注意：除权除息日前一天，正股行情进行修正)
    ON T.JYS = PG.JYS
   AND T.ZQDM = PG.ZQDM
 WHERE T.RQ = ${VAR:RQ};

 -----STEP 2:根据分级基金折算变更类数据生成中间表----
CREATE TABLE  TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_2
 AS
SELECT T.RQ,
       T.JYS,
       T.ZQDM,
       T.ZQMC,
       T.JYDW,
       CASE
           WHEN T.JYS = FJ_1.JYS AND T.ZQDM = FJ_1.ZQDM THEN
            FJ_1.JJJZ
           WHEN T.JYS = FJ_4.JYS AND T.ZQDM = FJ_4.ZQDM THEN
            T.ZSP - ROUND((IFNULL(FJ_4.FHPS, 1) - 1), 4)
           WHEN T.JYS = FJ_5.JYS AND T.ZQDM = FJ_5.ZQDM THEN
            IF(FJ_5.FHPS > 1,
               ROUND(T.ZSP - (FJ_5.FHPS - 1), 4),
               ROUND(T.ZSP / FJ_5.FHPS, 4))
           ELSE
            T.ZXJ
       END ZXJ,
       CASE
           WHEN T.JYS = FJ_2.JYS AND T.ZQDM = FJ_2.ZQDM THEN
            FJ_1.JJJZ
           WHEN T.JYS = FJ_3.JYS AND T.ZQDM = FJ_3.ZQDM AND IFNULL(T.ZSP, 0) = 0 THEN
            FJ_3.ZXJ
           ELSE
            T.ZSP
       END ZSP,
       T.JKP,
       T.ZGJ,
       T.ZDJ,
       T.CJSL,
       T.CJJE,
       T.ZXLX,
       T.ZXJ_ORG,
       T.ZSP_ORG,
       CASE
           WHEN T.JYS = FJ_1.JYS AND T.ZQDM = FJ_1.ZQDM THEN
            3
           WHEN (T.JYS = FJ_4.JYS AND T.ZQDM = FJ_4.ZQDM) OR
                (T.JYS = FJ_5.JYS AND T.ZQDM = FJ_5.ZQDM) THEN
            4
           ELSE
            GZJ_FLAG
       END GZJ_FLAG
  FROM TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_1 T
  LEFT JOIN (SELECT J.TADM AS JYS, J.JJDM AS ZQDM, J.JJJZ
               FROM INFO.THIS_JJJZ J, INFO.TFJJJXX X
              WHERE J.TADM = X.JYS
                AND J.JJDM = X.MJDM
                AND IFNULL(X.JYBS, 0) = 0
                AND J.JZRQ = ${VAR:RQ}) FJ_1 -- 将不可交易母基，用基金净值来覆盖当日的行情
    ON T.JYS = FJ_1.JYS
   AND T.ZQDM = FJ_1.ZQDM
  LEFT JOIN (SELECT J.TADM AS JYS, J.JJDM AS ZQDM, J.JJJZ
               FROM INFO.THIS_JJJZ J, INFO.TFJJJXX X
              WHERE J.TADM = X.JYS
                AND J.JJDM = X.MJDM
                AND IFNULL(X.JYBS, 0) = 0
                AND J.JZRQ = ${VAR:JYR_PRE1}) FJ_2 -- 将不可交易母基，用上日基金净值来覆盖当日的昨收价
    ON T.JYS = FJ_2.JYS
   AND T.ZQDM = FJ_2.ZQDM
  LEFT JOIN (SELECT RQ, ZXJ, JYS, ZQDM FROM DSC_BAS.T_ZQHQ_XZ_HIS WHERE RQ = ${VAR:JYR_PRE1}) FJ_3 -- 昨收盘为0的，用最新价替代
    ON T.JYS = FJ_3.JYS
   AND T.ZQDM = FJ_3.ZQDM
  LEFT JOIN (SELECT A2.JYS, A1.JJDM ZQDM, A1.FHPS
               FROM INFO.THIS_JJJZ A1, INFO.TFJJJXX A2
              WHERE A1.JZRQ = ${VAR:JYR_PRE1}
                AND A1.TADM = A2.JYS
                AND A1.JJDM = A2.AJDM
                AND (A1.FHPS IS NOT NULL AND A1.FHPS <> 0)
                AND IFNULL(A1.FHPS, 1) <> 1) FJ_4 -- 发生折算变更时，最新价重新计算A基
    ON T.JYS = FJ_4.JYS
   AND T.ZQDM = FJ_4.ZQDM
  LEFT JOIN (SELECT A2.JYS, A1.JJDM ZQDM, A1.FHPS
               FROM INFO.THIS_JJJZ A1, INFO.TFJJJXX A2
              WHERE A1.JZRQ = ${VAR:JYR_PRE1}
                AND A1.TADM = A2.JYS
                AND A1.JJDM = A2.BJDM
                AND (A1.FHPS IS NOT NULL AND A1.FHPS <> 0)
                AND IFNULL(A1.FHPS, 1) <> 1) FJ_5 -- 发生折算变更时，最新价重新计算B基
    ON T.JYS = FJ_5.JYS
   AND T.ZQDM = FJ_5.ZQDM;

 -----STEP 3:根据新股数据生成中间表----
-- 3.1 对于新股申购日期到上市日期之间的行情进行处理
CREATE TABLE  TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_3_1
 AS
SELECT GPDM,
       GPJC,
       SGDM,
       JYS,
       FXJ,
       SGRQ,
       ZQGBR,
       CASE
           WHEN SSRQ = 0 OR SSRQ IS NULL THEN
            20990101
           ELSE
            SSRQ
       END AS SSRQ
  FROM INFO.TXGGL --新股管理
 WHERE SGRQ <> 0
   AND SGRQ IS NOT NULL
   AND ABS(UDFS.F_GET_DAYDIFF(SGRQ, ${VAR:RQ})) < 180
   AND ${VAR:RQ} >= SGRQ
   AND ${VAR:RQ} < (CASE WHEN SSRQ = 0 OR SSRQ IS NULL THEN 20990101 ELSE SSRQ END);  -- 20191018

-- 3.2 对行情表中新股在中签公布日之前进行最新价和昨收盘字段修正
CREATE TABLE  TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_3
 AS
SELECT T.RQ,
       T.JYS,
       T.ZQDM,
       T.ZQMC,
       T.JYDW,
       CASE
           WHEN XG_1.SGDM IS NOT NULL AND ${VAR:RQ} < XG_1.ZQGBR AND SGRQ > 20160101  THEN
            0
           WHEN XG_1.SGDM IS NOT NULL THEN
            XG_1.FXJ
           ELSE
            T.ZXJ
       END ZXJ,
       CASE
           WHEN XG_1.SGDM IS NOT NULL AND ${VAR:RQ} < XG_1.ZQGBR AND SGRQ > 20160101  THEN
            0
           WHEN XG_1.SGDM IS NOT NULL THEN
            XG_1.FXJ
           ELSE
            T.ZSP
       END ZSP,
       T.JKP,
       T.ZGJ,
       T.ZDJ,
       T.CJSL,
       T.CJJE,
       T.ZXLX,
       CASE
           WHEN XG_1.SGDM IS NOT NULL AND ${VAR:RQ} < XG_1.ZQGBR AND SGRQ > 20160101  THEN
            0
           WHEN XG_1.SGDM IS NOT NULL  THEN
            XG_1.FXJ
           ELSE
            T.ZXJ
       END ZXJ_ORG,
       CASE
           WHEN XG_1.SGDM IS NOT NULL AND ${VAR:RQ} < XG_1.ZQGBR AND SGRQ > 20160101  THEN
            0
           WHEN XG_1.SGDM IS NOT NULL  THEN
            XG_1.FXJ
           ELSE
            T.ZSP
       END ZSP_ORG,
       T.GZJ_FLAG
  FROM TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_2 T  --修正了分级基金的行情数据
  LEFT JOIN (SELECT GPDM,
                    GPJC,
                    SGDM,
                    JYS,
                    FXJ,
                    SGRQ,
                    ZQGBR,
                    SSRQ
               FROM TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_3_1) XG_1   --新股行情
    ON T.JYS = XG_1.JYS
   AND T.ZQDM = XG_1.SGDM
 WHERE T.RQ = ${VAR:RQ};

-- 3.3 插入新股的行情数据
INSERT INTO TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_3

    SELECT ${VAR:RQ} AS RQ,
           D.JYS,
           D.SGDM AS ZQDM,
           D.GPJC AS ZQMC,
           1 AS JYDW,
           CASE
               WHEN ${VAR:RQ} < D.ZQGBR THEN
                0
               ELSE
                D.FXJ
           END AS ZXJ,
           CASE
               WHEN ${VAR:RQ} < D.ZQGBR THEN
                0
               ELSE
                D.FXJ
           END AS ZSP,
           0 AS JKP,
           0 AS ZGJ,
           0 AS ZDJ,
           0 AS CJSL,
           0 AS CJJE,
           0 AS ZXLX,
           CASE
               WHEN ${VAR:RQ} < D.ZQGBR THEN
                0
               ELSE
                D.FXJ
           END AS ZXJ_ORG,
           CASE
               WHEN ${VAR:RQ} < D.ZQGBR THEN
                0
               ELSE
                D.FXJ
           END AS ZSP_ORG,
           9 AS GZJ_FLAG
      FROM (SELECT GPDM, GPJC, SGDM, JYS, FXJ, SGRQ, ZQGBR, SSRQ
              FROM TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_3_1) D --新股行情
      LEFT JOIN (SELECT ZQDM
                   FROM TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_3 T --分级基金修正新股后的行情
                  WHERE T.RQ = ${VAR:RQ}
                    AND T.JYS IN ('1', '2')) HQ
        ON D.SGDM = HQ.ZQDM
     WHERE HQ.ZQDM IS NULL;   --20190715  此处关联作用？？


-- 3.4 上海新股需要额外处理
INSERT INTO TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_3

    SELECT ${VAR:RQ} AS RQ,
           D.JYS,
           D.GPDM AS ZQDM,
           D.GPJC AS ZQMC,
           1 AS JYDW,
           CASE
               WHEN ${VAR:RQ} < D.ZQGBR THEN
                0
               ELSE
                D.FXJ
           END AS ZXJ,
           CASE
               WHEN ${VAR:RQ} < D.ZQGBR THEN
                0
               ELSE
                D.FXJ
           END AS ZSP,
           0 AS JKP,
           0 AS ZGJ,
           0 AS ZDJ,
           0 AS CJSL,
           0 AS CJJE,
           0 AS ZXLX,
           CASE
               WHEN ${VAR:RQ} < D.ZQGBR THEN
                0
               ELSE
                D.FXJ
           END AS ZXJ_ORG,
           CASE
               WHEN ${VAR:RQ} < D.ZQGBR THEN
                0
               ELSE
                D.FXJ
           END AS ZSP_ORG,
           9 AS GZJ_FLAG
      FROM (SELECT GPDM, GPJC, SGDM, JYS, FXJ, SGRQ, ZQGBR, SSRQ
              FROM TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_3_1 --新股行情
             WHERE JYS = '2') D   --交易所:上海
      LEFT JOIN (SELECT ZQDM
                   FROM TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_3 T --分级基金+新股行情
                  WHERE T.RQ = ${VAR:RQ}
                    AND T.JYS IN ('2')) HQ
        ON D.GPDM = HQ.ZQDM
     WHERE HQ.ZQDM IS NULL;

-- 3.5 老新股申购制度下新股在申购后会生成申购缴款代码，需要额外处理
INSERT INTO TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_3

    SELECT ${VAR:RQ} AS RQ,
           D.JYS,
           D.GPDM AS ZQDM,
           D.GPJC AS ZQMC,
           1 AS JYDW,
           CASE
               WHEN ${VAR:RQ} < D.ZQGBR THEN
                0
               ELSE
                D.FXJ
           END AS ZXJ,
           CASE
               WHEN ${VAR:RQ} < D.ZQGBR THEN
                0
               ELSE
                D.FXJ
           END AS ZSP,
           0 AS JKP,
           0 AS ZGJ,
           0 AS ZDJ,
           0 AS CJSL,
           0 AS CJJE,
           0 AS ZXLX,
           CASE
               WHEN ${VAR:RQ} < D.ZQGBR THEN
                0
               ELSE
                D.FXJ
           END AS ZXJ_ORG,
           CASE
               WHEN ${VAR:RQ} < D.ZQGBR THEN
                0
               ELSE
                D.FXJ
           END AS ZSP_ORG,
           9 AS GZJ_FLAG
      FROM (SELECT CASE
                       WHEN SGRQ < 20160101 AND JYS = '2' THEN
                        CASE
                            WHEN SUBSTR(SGDM, 1, 3) = '732' THEN
                             CONCAT('734', SUBSTR(SGDM, 4, 3))
                            WHEN SUBSTR(SGDM, 1, 3) = '730' THEN
                             CONCAT('740', SUBSTR(SGDM, 4, 3))
                            WHEN SUBSTR(SGDM, 1, 3) = '780' THEN
                             CONCAT('790', SUBSTR(SGDM, 4, 3))
                        END
                       ELSE
                        SGDM
                   END AS SGJKDM,
                   GPDM,
                   GPJC,
                   SGDM,
                   JYS,
                   FXJ,
                   SGRQ,
                   ZQGBR,
                   SSRQ
              FROM TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_3_1 --新股行情
             WHERE SGRQ < 20160101) D --申购日期大于20160101的新股才有这种处理
      LEFT JOIN (SELECT ZQDM FROM TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_3 T WHERE T.RQ = ${VAR:RQ}) HQ
        ON D.SGJKDM = HQ.ZQDM
     WHERE D.SGJKDM IS NOT NULL
       AND HQ.ZQDM IS NULL;

 -----STEP 4:根据其他数据生成中间表（如L0行情缺失、证券代码变更、可转债等）----
 -- 4.1 证券代码存在，但是行情获取不到的L0部分行情，并标识为99
INSERT INTO TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_3
    (RQ,
     JYS,
     ZQDM,
     ZQMC,
     JYDW,
     ZXJ,
     ZSP,
     JKP,
     ZGJ,
     ZDJ,
     CJSL,
     CJJE,
     ZXLX,
     ZXJ_ORG,
     ZSP_ORG,
     GZJ_FLAG)
    SELECT ${VAR:RQ} AS RQ,
           T.JYS,
           T.ZQDM,
           T.ZQMC,
           T.JYDW,
           D.ZXJ  AS ZXJ,
           D.ZSP  AS ZSP,
           0      AS JKP,
           0      AS ZGJ,
           0      AS ZDJ,
           0      AS CJSL,
           0      AS CJJE,
           0      AS ZXLX,
           D.ZXJ  AS ZXJ_ORG,
           D.ZSP  AS ZSP_ORG,
           99     AS GZJ_FLAG
      FROM DSC_CFG.T_ZQDM T
      LEFT JOIN (SELECT JYS, ZQDM, ZXJ, ZSP FROM DSC_BAS.T_ZQHQ_HIS WHERE RQ = ${VAR:RQ}) D
        ON T.JYS = D.JYS
       AND T.ZQDM = D.ZQDM
     WHERE ${VAR:RQ} >= FXRQ   --发行日期
       AND T.ZQLB = 'L0'
       AND D.ZQDM IS NULL;


 -- 4.2 证券代码发生变更转换的
INSERT INTO TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_3
    (RQ,
     JYS,
     ZQDM,
     ZQMC,
     JYDW,
     ZXJ,
     ZSP,
     JKP,
     ZGJ,
     ZDJ,
     CJSL,
     CJJE,
     ZXLX,
     ZXJ_ORG,
     ZSP_ORG,
     GZJ_FLAG)
    SELECT ${VAR:RQ} AS RQ,
           T.JYS,
           T.BGDM AS ZQDM,
           T.BGMC AS ZQMC,
           1      AS JYDW,
           D.ZXJ  AS ZXJ,
           D.ZSP  AS ZSP,
           0      AS JKP,
           0      AS ZGJ,
           0      AS ZDJ,
           0      AS CJSL,
           0      AS CJJE,
           0      AS ZXLX,
           D.ZXJ  AS ZXJ_ORG,
           D.ZSP  AS ZSP_ORG,
           99     AS GZJ_FLAG
      FROM INFO.TZQDMBG T, --证券代码变更
           (SELECT JYS,
                   ZQDM,
                   ZXJ,
                   ZSP
              FROM DSC_BAS.T_ZQHQ_HIS T
             WHERE RQ = ${VAR:RQ}) D
     WHERE T.JYS = D.JYS
       AND T.ZQDM = D.ZQDM
       AND T.BGRQ = ${VAR:RQ};

-- 4.3 上海可转债上市日-1日补一条债券代码行情
INSERT INTO TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_3
    (RQ,
     JYS,
     ZQDM,
     ZQMC,
     JYDW,
     ZXJ,
     ZSP,
     JKP,
     ZGJ,
     ZDJ,
     CJSL,
     CJJE,
     ZXLX,
     ZXJ_ORG,
     ZSP_ORG,
     GZJ_FLAG)
    SELECT ${VAR:RQ} AS RQ,
           K.JYS,
           K.ZQDM,
           K.ZQMC,
           CASE
               WHEN K.JYS = '2' THEN
                10
               ELSE
                1
           END AS JYDW, --调整
           K.FXJ AS ZXJ,
           K.FXJ AS ZSP,
           0 AS JKP,
           0 AS ZGJ,
           0 AS ZDJ,
           0 AS CJSL,
           0 AS CJJE,
           0 AS ZXLX,
           K.FXJ AS ZXJ_ORG,
           K.FXJ AS ZSP_ORG,
           10 AS GZJ_FLAG
      FROM (SELECT CASE
                       WHEN SUBSTR(GPDM, 1, 1) = '6' THEN
                        '2' --上海
                       WHEN SUBSTR(GPDM, 1, 1) = '0' THEN
                        '1' --深圳
                       WHEN SUBSTR(GPDM, 1, 1) = '3' THEN --20190725新发现的有债券以3开头
                        '1' --深圳
                       ELSE
                        NULL
                   END AS JYS,
                   ZQDM,
                   ZQJC AS ZQMC,
                   FXJ
              FROM INFO.TZQ_KZZ
             WHERE SSRQ IS NOT NULL
               AND SSRQ <> 0
               AND SSRQ = ${VAR:JYR_AFT1}) K -- 注意：这里需要用到获取前后N个交易日对应日期的函数
      LEFT JOIN (SELECT JYS, ZQDM, ZXJ, ZSP FROM DSC_BAS.T_ZQHQ_HIS WHERE RQ = ${VAR:RQ}) D
        ON K.JYS = D.JYS
       AND K.ZQDM = D.ZQDM
     WHERE K.JYS = '2' --上海
       AND D.ZQDM IS NULL;  --过滤已有上海债券行情

--20191025 可转债申购旧规：债券代码行情中签公布日+1至上市日-1日需要补行情
INSERT INTO TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_3
    (RQ,
     JYS,
     ZQDM,
     ZQMC,
     JYDW,
     ZXJ,
     ZSP,
     JKP,
     ZGJ,
     ZDJ,
     CJSL,
     CJJE,
     ZXLX,
     ZXJ_ORG,
     ZSP_ORG,
     GZJ_FLAG)
    SELECT ${VAR:RQ} AS RQ,
           K.JYS,
           K.ZQDM,
           K.ZQMC,
           CASE
               WHEN K.JYS = '2' THEN
                10
               ELSE
                1
           END AS JYDW, --调整
           K.FXJ AS ZXJ,
           K.FXJ AS ZSP,
           0 AS JKP,
           0 AS ZGJ,
           0 AS ZDJ,
           0 AS CJSL,
           0 AS CJJE,
           0 AS ZXLX,
           K.FXJ AS ZXJ_ORG,
           K.FXJ AS ZSP_ORG,
           10 AS GZJ_FLAG
      FROM (SELECT CASE
                       WHEN SUBSTR(GPDM, 1, 1) = '6' THEN
                        '2' --上海
                       WHEN SUBSTR(GPDM, 1, 1) = '0' THEN
                        '1' --深圳
                       WHEN SUBSTR(GPDM, 1, 1) = '3' THEN --20190725新发现的有债券以3开头
                        '1' --深圳
                       ELSE
                        NULL
                   END AS JYS,
                   ZQDM,
                   ZQJC AS ZQMC,
                   FXJ
              FROM INFO.TZQ_KZZ
             WHERE ${VAR:RQ} BETWEEN UDFS.F_GET_JYR_DATE(ZQHGBR, 1) AND
                   UDFS.F_GET_JYR_DATE(SSRQ, -1)  -- 注意：中签公布日+1至上市日-1
               AND ZQHGBR IS NOT NULL
               AND ZQHGBR <> 0
               AND SSRQ IS NOT NULL
               AND SSRQ <> 0) K
      LEFT JOIN (SELECT JYS, ZQDM, ZXJ, ZSP FROM DSC_BAS.T_ZQHQ_HIS WHERE RQ = ${VAR:RQ}) D
        ON K.JYS = D.JYS
       AND K.ZQDM = D.ZQDM
     WHERE K.JYS = '1' -- 深圳
       AND D.ZQDM IS NULL;   --过滤已有的深圳债券行情

 -- 4.4 ETF认购代码缺失的补充
INSERT INTO TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_3
    (RQ,
     JYS,
     ZQDM,
     ZQMC,
     JYDW,
     ZXJ,
     ZSP,
     JKP,
     ZGJ,
     ZDJ,
     CJSL,
     CJJE,
     ZXLX,
     ZXJ_ORG,
     ZSP_ORG,
     GZJ_FLAG)
    SELECT ${VAR:RQ} AS RQ,
           T.JYS,
           T.ZQDM,
           T.ZQMC,
           T.JYDW,
           T.ZXJ,
           T.ZSP,
           T.JKP,
           T.ZGJ,
           T.ZDJ,
           T.CJSL,
           CAST(T.CJJE AS DECIMAL(16,2)) AS CJJE,
           T.ZXLX,
           T.ZXJ  AS ZXJ_ORG,
           T.ZSP  AS ZSP_ORG,
           99     AS GZJ_FLAG
      FROM DSC_BAS.T_ZQHQ_XZ_HIS T
 LEFT JOIN INFO.TETF_JBXX E ON (T.JYS = E.JYS AND (T.ZQDM = E.JJDM OR T.ZQDM = CAST((CAST(E.JJDM AS INT) + 3) AS STRING)))
 LEFT JOIN TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_3 TEMPTEST ON (T.JYS = TEMPTEST.JYS AND T.ZQDM = TEMPTEST.ZQDM)
     WHERE T.RQ = ${VAR:JYR_PRE1}
	   AND T.JYS = '2'
	   AND E.JJDM IS NOT NULL
	   AND TEMPTEST.ZQDM IS NULL;
	   
-- 20200307 港股的行情不修正为人民币	   
/* -----STEP 5:根据其他数据生成中间表（如可转债、港股通股票最新价、对非折算变更停牌的A、B基、股票用昨收盘来覆盖最新价）----
CREATE TABLE  TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_4
AS
SELECT T.RQ,
       T.JYS,
       T.ZQDM,
       T.ZQMC,
       T.JYDW,
       CASE
           WHEN JYS IN ('8', '9') THEN
            ROUND(ZXJ * D.MRHL, 4)
           WHEN IFNULL(ZXJ, 0) = 0 AND ZSP <> 0 AND GZJ_FLAG = 1 THEN
            ZSP
           ELSE
            ZXJ
       END ZXJ,
       CASE
           WHEN JYS IN ('8', '9') THEN
            ROUND(ZSP * D.MRHL, 4)
           ELSE
            ZSP
       END ZSP,
       T.JKP,
       T.ZGJ,
       T.ZDJ,
       T.CJSL,
       T.CJJE,
       T.ZXLX,
       T.ZXJ_ORG,
       T.ZSP_ORG,
       T.GZJ_FLAG
  FROM (SELECT A.*,
               CASE
                   WHEN JYS IN ('8', '9') THEN
                    1
                   ELSE
                    0
               END AS GG_FLAG
          FROM TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_3 A) T
  LEFT JOIN (SELECT XHMRJ AS MRHL, 1 AS GG_FLAG
               FROM DSC_CFG.T_HLCS_EXT
              WHERE BZ = '2'
                AND RQ = ${VAR:RQ}) D
    ON T.GG_FLAG = D.GG_FLAG;
*/


--20190820
--20191025 添加可转债申购新规执行日期限定20170901
-----STEP 6:可转债申购代码 申购日期至中签公布日行情修正为0
CREATE TABLE TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_5
AS
SELECT T.RQ,
       T.JYS,
       T.ZQDM,
       T.ZQMC,
       T.JYDW,
       CASE
           WHEN ${VAR:RQ} >= 20170901 AND KZZ_SG.ZQDM IS NOT NULL THEN  --20191025
            0
           ELSE
            T.ZXJ
       END ZXJ,
       CASE
           WHEN ${VAR:RQ} >= 20170901 AND  KZZ_SG.ZQDM IS NOT NULL THEN  --20191025
            0
           ELSE
            T.ZSP
       END ZSP,
       CASE
           WHEN  ${VAR:RQ} >= 20170901 AND KZZ_SG.ZQDM IS NOT NULL THEN  --20191025
            0
           ELSE
            T.JKP
       END AS JKP,
       CASE
           WHEN ${VAR:RQ} >= 20170901 AND KZZ_SG.ZQDM IS NOT NULL THEN  --20191025
            0
           ELSE
            T.ZGJ
       END AS ZGJ,
       CASE
           WHEN ${VAR:RQ} >= 20170901 AND KZZ_SG.ZQDM IS NOT NULL THEN  --20191025
            0
           ELSE
            T.ZDJ
       END AS ZDJ,
       T.CJSL,
       T.CJJE,
       T.ZXLX,
       T.ZXJ_ORG,
       T.ZSP_ORG,
       T.GZJ_FLAG
  FROM TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_3 T  -- 20200307 废弃TEMPTEST_T_ZQHQ_XZ_HIS_4
  LEFT JOIN (SELECT SGDM AS ZQDM,
                    CASE
                        WHEN SUBSTR(GPDM, 1, 1) = '6' THEN
                         '2'
                        WHEN SUBSTR(GPDM, 1, 1) IN ('0', '3') THEN
                         '1'
                        ELSE
                         ''
                    END AS JYS
               FROM INFO.TZQ_KZZ T
              WHERE SGDM IS NOT NULL
                AND ${VAR:RQ} BETWEEN SGRQ AND ZQHGBR) KZZ_SG
    ON T.JYS = KZZ_SG.JYS
   AND T.ZQDM = KZZ_SG.ZQDM;

-- 20190904
-----STEP 7:除权除息前一日直到上市前一日，配股行情修正为与正股价格一致且过滤上市后的配股行情
CREATE TABLE TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_6
AS
SELECT RQ,
       JYS,
       ZQDM,
       ZQMC,
       JYDW,
       ZXJ,
       ZSP,
       JKP,
       ZGJ,
       ZDJ,
       CJSL,
       CJJE,
       ZXLX,
       ZXJ_ORG,
       ZSP_ORG,
       GZJ_FLAG
  FROM (SELECT PSHQ.RQ      AS RQ,
               PSHQ.JYS     AS JYS,
               PG.PSDM      AS ZQDM,
               PSHQ.ZQMC    AS ZQMC,
               PSHQ.JYDW    AS JYDW,
               ZGHQ.ZXJ     AS ZXJ,
               ZGHQ.ZSP     AS ZSP,
               PSHQ.JKP     AS JKP,
               PSHQ.ZGJ     AS ZGJ,
               PSHQ.ZDJ     AS ZDJ,
               PSHQ.CJSL    AS CJSL,
               PSHQ.CJJE    AS CJJE,
               PSHQ.ZXLX    AS ZXLX,
               PSHQ.ZXJ_ORG AS ZXJ_ORG,
               PSHQ.ZSP_ORG AS ZSP_ORG,
               8            AS GZJ_FLAG
          FROM (SELECT JYS, ZQDM, PSDM
                  FROM INFO.TGP_GPPG --股票-股票配股信息
                 WHERE CQCXRQ IS NOT NULL
                   AND CQCXRQ <> 0
                   AND PGSSRQ IS NOT NULL
                   AND PGSSRQ <> 0
                   AND PSDM IS NOT NULL
                   AND ${VAR:RQ} BETWEEN UDFS.F_GET_JYR_DATE(CQCXRQ, -1) AND
                       UDFS.F_GET_JYR_DATE(PGSSRQ, -1)) PG --配股
          JOIN TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_5 PSHQ --配股行情
            ON PSHQ.JYS = PG.JYS
           AND PSHQ.ZQDM = PG.PSDM
           AND PSHQ.RQ = ${VAR:RQ}
          JOIN TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_5 ZGHQ --正股行情
            ON PG.JYS = ZGHQ.JYS
           AND PG.ZQDM = ZGHQ.ZQDM
           AND ZGHQ.RQ = ${VAR:RQ}
        UNION ALL
        SELECT T.RQ,
               T.JYS,
               T.ZQDM,
               T.ZQMC,
               T.JYDW,
               T.ZXJ,
               T.ZSP,
               T.JKP,
               T.ZGJ,
               T.ZDJ,
               T.CJSL,
               T.CJJE,
               T.ZXLX,
               T.ZXJ_ORG,
               T.ZSP_ORG,
               T.GZJ_FLAG
          FROM TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_5 T
         WHERE T.RQ = ${VAR:RQ}
           AND NOT EXISTS
         (SELECT 1
                  FROM INFO.TGP_GPPG PG --股票-股票配股信息
                 WHERE PG.CQCXRQ IS NOT NULL
                   AND PG.CQCXRQ <> 0
                   AND PG.PGSSRQ IS NOT NULL
                   AND PG.PGSSRQ <> 0
                   AND PG.PSDM IS NOT NULL
                   AND ${VAR:RQ} BETWEEN UDFS.F_GET_JYR_DATE(PG.CQCXRQ, -1) AND
                       UDFS.F_GET_JYR_DATE(CAST(CASE WHEN PG.PGSSRQ IS NULL OR PG.PGSSRQ=0 THEN 30000101 ELSE PG.PGSSRQ END AS INT), -1) --20200513
                   AND T.JYS = PG.JYS
                   AND T.ZQDM = PG.PSDM)) T1
 WHERE NOT EXISTS (SELECT 1
          FROM INFO.TGP_GPPG PG --股票-股票配股信息
         WHERE PG.CQCXRQ IS NOT NULL
           AND PG.CQCXRQ <> 0
           AND PG.PGSSRQ IS NOT NULL
           AND PG.PGSSRQ <> 0
           AND PG.PSDM IS NOT NULL
           AND ${VAR:RQ} >= PG.PGSSRQ  --上市配股行情
		   AND ABS(PG.PGSSRQ - ${VAR:RQ}) < 600  --20200513
           AND T1.JYS = PG.JYS
           AND T1.ZQDM = PG.PSDM);

-----STEP 8:生成目标表
INSERT OVERWRITE TABLE DSC_BAS.T_ZQHQ_XZ_HIS
     PARTITION
    (RQ = ${VAR:RQ})
    SELECT T.JYS,
           T.ZQDM,
           NVL(D.ZQMC, T.ZQMC) AS ZQMC,
           T.JYDW,
           CAST(T.ZXJ AS DECIMAL(9, 4)) AS ZXJ,
           CAST(T.ZSP AS DECIMAL(9, 4)) AS ZSP,
           T.JKP,
           T.ZGJ,
           T.ZDJ,
           T.CJSL,
           T.CJJE,
           CASE
               WHEN T.ZXLX = 0 AND NVL(A.ZXLX, 0) <> 0 AND A.ZXLX IS NOT NULL THEN
                A.ZXLX
               ELSE
                T.ZXLX
           END ZXLX,
           CAST(ROUND((CASE
                          WHEN T.ZXLX = 0 AND NVL(A.ZXLX, 0) <> 0 AND A.ZXLX IS NOT NULL THEN
                           A.ZXLX
                          ELSE
                           T.ZXLX
                      END),
                      6) AS DECIMAL(16, 6)) AS LXJG,
           CAST(D.JJJYBZ AS INT) AS JJJYBZ,
           CAST(T.ZXJ_ORG AS DECIMAL(9, 4)) AS ZXJ_ORG,
           CAST(T.ZSP_ORG AS DECIMAL(9, 4)) AS ZSP_ORG,
           T.GZJ_FLAG
      FROM TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_6 T
      LEFT JOIN DSC_CFG.T_ZQDM D
        ON T.JYS = D.JYS
       AND T.ZQDM = D.ZQDM
      LEFT JOIN (SELECT UDFS.F_GET_ETL_TRAN_DICVAL('DSC_CFG', 'T_ZQDM', 'JYS', 1, 1, A.JYS) AS JYS,
                        TRIM(ZQDM) AS ZQDM,
                        ZXLX
                   FROM SRC_ABOSS.TZQDM A) A --通过SRC层表修复ZXLX，支持历史重跑
        ON T.JYS = A.JYS
       AND T.ZQDM = A.ZQDM;


-----STEP 99:删除临时表
DROP TABLE IF EXISTS TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_1;
DROP TABLE IF EXISTS TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_2;
DROP TABLE IF EXISTS TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_3;
DROP TABLE IF EXISTS TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_3_1;
DROP TABLE IF EXISTS TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_4;
DROP TABLE IF EXISTS TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_5;
DROP TABLE IF EXISTS TEMPTEST.TEMPTEST_T_ZQHQ_XZ_HIS_6;

