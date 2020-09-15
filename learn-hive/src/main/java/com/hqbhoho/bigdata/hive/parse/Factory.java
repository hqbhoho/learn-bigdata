package com.hqbhoho.bigdata.hive.parse;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.HiveDriver;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.processors.*;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.sql.SQLException;
import java.util.*;

import static jodd.util.StringUtil.isBlank;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/24
 */
public class Factory {
    private Factory() {
        // prevent instantiation
    }

    private static final Map<HiveConf, HiveDriver> mapDrivers = Collections.synchronizedMap(new HashMap<HiveConf, HiveDriver>());

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
        if (result != null) {
            return result;
        }
        if (isBlank(cmd[0])) {
            return null;
        } else {
            if (conf == null) {
                return new HiveDriver();
            }
            HiveDriver drv = mapDrivers.get(conf);
            if (drv == null) {
                drv = new HiveDriver();
                mapDrivers.put(conf, drv);
            }
            drv.init();
            return drv;
        }
    }

    public static void clean(HiveConf conf) {
        HiveDriver drv = mapDrivers.get(conf);
        if (drv != null) {
            drv.destroy();
        }

        mapDrivers.remove(conf);
    }
}
