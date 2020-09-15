package com.hqbhoho.bigdata.hive.parse1;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.HiveProcessDriver;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.processors.*;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.sql.SQLException;
import java.util.*;

import static org.apache.commons.lang.StringUtils.isBlank;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/05/19
 */
public final class HiveCommandProcessorFactory {

    private HiveCommandProcessorFactory() {
        // prevent instantiation
    }

    // TODO 修改Driver -> HiveProcessDriver
    private static final Map<HiveConf, HiveProcessDriver> mapDrivers = Collections.synchronizedMap(new HashMap<HiveConf, HiveProcessDriver>());

    public static CommandProcessor get(String cmd)
            throws SQLException {
        return get(new String[]{cmd}, null);
    }

    public static CommandProcessor getForHiveCommand(String[] cmd, HiveConf conf)
            throws SQLException {
        return getForHiveCommandInternal(cmd, conf, false);
    }

    public static CommandProcessor getForHiveCommandInternal(String[] cmd, HiveConf conf,
                                                             boolean testOnly)
            throws SQLException {
        HiveCommand hiveCommand = HiveCommand.find(cmd, testOnly);
        if (hiveCommand == null || isBlank(cmd[0])) {
            return null;
        }
        if (conf == null) {
            conf = new HiveConf();
        }
        Set<String> availableCommands = new HashSet<String>();
        for (String availableCommand : conf.getVar(HiveConf.ConfVars.HIVE_SECURITY_COMMAND_WHITELIST)
                .split(",")) {
            availableCommands.add(availableCommand.toLowerCase().trim());
        }
        if (!availableCommands.contains(cmd[0].trim().toLowerCase())) {
            throw new SQLException("Insufficient privileges to execute " + cmd[0], "42000");
        }
        switch (hiveCommand) {
            case SET:
                return new SetProcessor();
            case RESET:
                return new ResetProcessor();
            case DFS:
                SessionState ss = SessionState.get();
                return new DfsProcessor(ss.getConf());
            case ADD:
                return new AddResourceProcessor();
            case LIST:
                return new ListResourceProcessor();
            case DELETE:
                return new DeleteResourceProcessor();
            case COMPILE:
                return new CompileProcessor();
            case RELOAD:
                return new ReloadProcessor();
            case CRYPTO:
                try {
                    return new CryptoProcessor(SessionState.get().getHdfsEncryptionShim(), conf);
                } catch (HiveException e) {
                    throw new SQLException("Fail to start the command processor due to the exception: ", e);
                }
            default:
                throw new AssertionError("Unknown HiveCommand " + hiveCommand);
        }
    }

    public static CommandProcessor get(String[] cmd, HiveConf conf)
            throws SQLException {
        CommandProcessor result = getForHiveCommand(cmd, conf);
        // TODO   自定义一个HiveProcessDriver  实现对MR TASK进行空实现  不执行相应的TASK
        if (result != null) {
            return result;
        }
        if (isBlank(cmd[0])) {
            return null;
        } else {
            if (conf == null) {
                return new HiveProcessDriver();
                /*return new Driver();*/
            }
            HiveProcessDriver drv = mapDrivers.get(conf);
            if (drv == null) {
                /*drv = new Driver();*/
                drv = new HiveProcessDriver();
                mapDrivers.put(conf, drv);
            }
            drv.init();
            return drv;
        }
    }

    public static void clean(HiveConf conf) {
        HiveProcessDriver drv = mapDrivers.get(conf);
        if (drv != null) {
            drv.destroy();
        }

        mapDrivers.remove(conf);
    }
}
