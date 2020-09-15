package org.apache.hadoop.hive.ql;
import org.antlr.runtime.TokenRewriteStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.common.BlobStorageUtils;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils.Operation;
import org.apache.hadoop.hive.ql.lockmgr.HiveLock;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockManager;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObj;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.util.StringUtils;

import java.io.DataInput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class Context {
    private boolean isHDFSCleanup;
    private Path resFile;
    private Path resDir;
    private FileSystem resFs;
    private static final Log LOG = LogFactory.getLog("hive.ql.Context");
    private Path[] resDirPaths;
    private int resDirFilesNum;
    boolean initialized;
    String originalTracker;
    private final Map<String, ContentSummary> pathToCS;
    private final Path nonLocalScratchPath;
    private final String localScratchDir;
    private final String scratchDirPermission;
    private final Map<String, Path> fsScratchDirs;
    private final Configuration conf;
    protected int pathid;
    protected boolean explain;
    protected boolean explainLogical;
    protected String cmd;
    protected int tryCount;
    private TokenRewriteStream tokenRewriteStream;
    private final String executionId;
    protected List<HiveLock> hiveLocks;
    protected HiveLockManager hiveLockMgr;
    protected HiveTxnManager hiveTxnManager;
    private Operation acidOperation;
    private boolean needLockMgr;
    private final Map<LoadTableDesc, WriteEntity> loadTableOutputMap;
    private final Map<WriteEntity, List<HiveLockObj>> outputLockObjects;
    private final String stagingDir;
    private static final String MR_PREFIX = "-mr-";
    private static final String EXT_PREFIX = "-ext-";
    private static final String LOCAL_PREFIX = "-local-";

    public Context(Configuration conf) throws IOException {
        this(conf, generateExecutionId());
    }

    public Context(Configuration conf, String executionId) {
        this.originalTracker = null;
        this.pathToCS = new ConcurrentHashMap();
        this.fsScratchDirs = new HashMap();
        this.pathid = 10000;
        this.explain = false;
        this.explainLogical = false;
        this.cmd = "";
        this.tryCount = 0;
        this.acidOperation = Operation.NOT_ACID;
        this.loadTableOutputMap = new HashMap();
        this.outputLockObjects = new HashMap();
        this.conf = conf;
        this.executionId = executionId;
        this.nonLocalScratchPath = new Path(SessionState.getHDFSSessionPath(conf), executionId);
        this.localScratchDir = (new Path(SessionState.getLocalSessionPath(conf), executionId)).toUri().getPath();
        this.scratchDirPermission = HiveConf.getVar(conf, ConfVars.SCRATCHDIRPERMISSION);
        this.stagingDir = HiveConf.getVar(conf, ConfVars.STAGINGDIR);
    }

    public Map<LoadTableDesc, WriteEntity> getLoadTableOutputMap() {
        return this.loadTableOutputMap;
    }

    public Map<WriteEntity, List<HiveLockObj>> getOutputLockObjects() {
        return this.outputLockObjects;
    }

    public void setExplain(boolean value) {
        this.explain = value;
    }

    public boolean getExplain() {
        return this.explain;
    }

    public boolean getExplainLogical() {
        return this.explainLogical;
    }

    public void setExplainLogical(boolean explainLogical) {
        this.explainLogical = explainLogical;
    }

    public void setCmd(String cmd) {
        this.cmd = cmd;
    }

    public String getCmd() {
        return this.cmd;
    }

    private Path getStagingDir(Path inputPath, boolean mkdir) {
        URI inputPathUri = inputPath.toUri();
        String inputPathName = inputPathUri.getPath();
        String fileSystem = inputPathUri.getScheme() + ":" + inputPathUri.getAuthority();

        FileSystem fs;
        try {
            fs = inputPath.getFileSystem(this.conf);
        } catch (IOException var12) {
            throw new IllegalStateException("Error getting FileSystem for " + inputPath + ": " + var12, var12);
        }

        String stagingPathName;
        if (inputPathName.indexOf(this.stagingDir) == -1) {
            stagingPathName = (new Path(inputPathName, this.stagingDir)).toString();
        } else {
            stagingPathName = inputPathName.substring(0, inputPathName.indexOf(this.stagingDir) + this.stagingDir.length());
        }

        String key = fileSystem + "-" + stagingPathName + "-" + TaskRunner.getTaskRunnerID();
        Path dir = (Path)this.fsScratchDirs.get(key);
        if (dir == null) {
            dir = fs.makeQualified(new Path(stagingPathName + "_" + this.executionId + "-" + TaskRunner.getTaskRunnerID()));
            LOG.debug("Created staging dir = " + dir + " for path = " + inputPath);
            if (mkdir) {
                // todo  2020-05-19  去除创建staging目录的动作
                LOG.info("Lineage Analyzer don't create staging dir = " + dir + " for path = " + inputPath);

               /* try {
                    *//*if (!FileUtils.mkdir(fs, dir, true, this.conf)) {
                        throw new IllegalStateException("Cannot create staging directory  '" + dir.toString() + "'");
                    }*//*

                    if (this.isHDFSCleanup) {
                        fs.deleteOnExit(dir);

                    }
                } catch (IOException var11) {
                    throw new RuntimeException("Cannot create staging directory '" + dir.toString() + "': " + var11.getMessage(), var11);
                }*/
            }

            this.fsScratchDirs.put(key, dir);
        }

        return dir;
    }

    private Path getScratchDir(String scheme, String authority, boolean mkdir, String scratchDir) {
        String fileSystem = scheme + ":" + authority;
        Path dir = (Path)this.fsScratchDirs.get(fileSystem + "-" + TaskRunner.getTaskRunnerID());
        if (dir == null) {
            Path dirPath = new Path(scheme, authority, scratchDir + "-" + TaskRunner.getTaskRunnerID());
            if (mkdir) {
                try {
                    FileSystem fs = dirPath.getFileSystem(this.conf);
                    dirPath = new Path(fs.makeQualified(dirPath).toString());
                    FsPermission fsPermission = new FsPermission(this.scratchDirPermission);
                    if (!fs.mkdirs(dirPath, fsPermission)) {
                        throw new RuntimeException("Cannot make directory: " + dirPath.toString());
                    }

                    if (this.isHDFSCleanup) {
                        fs.deleteOnExit(dirPath);
                    }
                } catch (IOException var10) {
                    throw new RuntimeException(var10);
                }
            }

            dir = dirPath;
            this.fsScratchDirs.put(fileSystem + "-" + TaskRunner.getTaskRunnerID(), dirPath);
        }

        return dir;
    }

    public Path getLocalScratchDir(boolean mkdir) {
        try {
            FileSystem fs = FileSystem.getLocal(this.conf);
            URI uri = fs.getUri();
            return this.getScratchDir(uri.getScheme(), uri.getAuthority(), mkdir, this.localScratchDir);
        } catch (IOException var4) {
            throw new RuntimeException(var4);
        }
    }

    public Path getMRScratchDir() {
        if (this.isLocalOnlyExecutionMode()) {
            return this.getLocalScratchDir(!this.explain);
        } else {
            try {
                Path dir = FileUtils.makeQualified(this.nonLocalScratchPath, this.conf);
                URI uri = dir.toUri();
                Path newScratchDir = this.getScratchDir(uri.getScheme(), uri.getAuthority(), !this.explain, uri.getPath());
                LOG.info("New scratch dir is " + newScratchDir);
                return newScratchDir;
            } catch (IOException var4) {
                throw new RuntimeException(var4);
            } catch (IllegalArgumentException var5) {
                throw new RuntimeException("Error while making MR scratch directory - check filesystem config (" + var5.getCause() + ")", var5);
            }
        }
    }

    public Path getTempDirForPath(Path path, boolean isFinalJob) {
        return (!BlobStorageUtils.isBlobStoragePath(this.conf, path) || BlobStorageUtils.isBlobStorageAsScratchDir(this.conf)) && !this.isPathLocal(path) || isFinalJob && BlobStorageUtils.areOptimizationsEnabled(this.conf) ? this.getExtTmpPathRelTo(path) : this.getMRTmpPath();
    }

    public Path getTempDirForPath(Path path) {
        return this.getTempDirForPath(path, false);
    }

    private boolean isPathLocal(Path path) {
        boolean isLocal = false;
        if (path != null) {
            String scheme = path.toUri().getScheme();
            if (scheme != null) {
                isLocal = scheme.equals("file:///");
            }
        }

        return isLocal;
    }

    private Path getExternalScratchDir(URI extURI) {
        return this.getStagingDir(new Path(extURI.getScheme(), extURI.getAuthority(), extURI.getPath()), !this.explain);
    }

    public void removeScratchDir() {
        Iterator i$ = this.fsScratchDirs.entrySet().iterator();

        while(i$.hasNext()) {
            Entry entry = (Entry)i$.next();

            try {
                Path p = (Path)entry.getValue();
                FileSystem fs = p.getFileSystem(this.conf);
                LOG.debug("Deleting scratch dir: " + p);
                fs.delete(p, true);
                fs.cancelDeleteOnExit(p);
            } catch (Exception var5) {
                LOG.warn("Error Removing Scratch: " + StringUtils.stringifyException(var5));
            }
        }

        this.fsScratchDirs.clear();
    }

    private String nextPathId() {
        return Integer.toString(this.pathid++);
    }

    public boolean isMRTmpFileURI(String uriStr) {
        return uriStr.indexOf(this.executionId) != -1 && uriStr.indexOf("-mr-") != -1;
    }

    public Path getMRTmpPath(URI uri) {
        return new Path(this.getStagingDir(new Path(uri), !this.explain), "-mr-" + this.nextPathId());
    }

    public Path getMRTmpPath() {
        return new Path(this.getMRScratchDir(), "-mr-" + this.nextPathId());
    }

    public Path getLocalTmpPath() {
        return new Path(this.getLocalScratchDir(true), "-local-" + this.nextPathId());
    }

    public Path getExternalTmpPath(Path path) {
        URI extURI = path.toUri();
        return extURI.getScheme().equals("viewfs") ? this.getExtTmpPathRelTo(path.getParent()) : new Path(this.getExternalScratchDir(extURI), "-ext-" + this.nextPathId());
    }

    public Path getExtTmpPathRelTo(Path path) {
        return new Path(this.getStagingDir(path, !this.explain), "-ext-" + this.nextPathId());
    }

    public Path getResFile() {
        return this.resFile;
    }

    public void setResFile(Path resFile) {
        this.resFile = resFile;
        this.resDir = null;
        this.resDirPaths = null;
        this.resDirFilesNum = 0;
    }

    public Path getResDir() {
        return this.resDir;
    }

    public void setResDir(Path resDir) {
        this.resDir = resDir;
        this.resFile = null;
        this.resDirFilesNum = 0;
        this.resDirPaths = null;
    }

    public void clear() throws IOException {
        FileSystem fs;
        if (this.resDir != null) {
            try {
                fs = this.resDir.getFileSystem(this.conf);
                LOG.debug("Deleting result dir: " + this.resDir);
                fs.delete(this.resDir, true);
            } catch (IOException var3) {
                LOG.info("Context clear error: " + StringUtils.stringifyException(var3));
            }
        }

        if (this.resFile != null) {
            try {
                fs = this.resFile.getFileSystem(this.conf);
                LOG.debug("Deleting result file: " + this.resFile);
                fs.delete(this.resFile, false);
            } catch (IOException var2) {
                LOG.info("Context clear error: " + StringUtils.stringifyException(var2));
            }
        }

        this.removeScratchDir();
        this.originalTracker = null;
        this.setNeedLockMgr(false);
    }

    public DataInput getStream() {
        try {
            if (!this.initialized) {
                this.initialized = true;
                if (this.resFile == null && this.resDir == null) {
                    return null;
                } else if (this.resFile != null) {
                    return this.resFile.getFileSystem(this.conf).open(this.resFile);
                } else {
                    this.resFs = this.resDir.getFileSystem(this.conf);
                    FileStatus status = this.resFs.getFileStatus(this.resDir);

                    assert status.isDir();

                    FileStatus[] resDirFS = this.resFs.globStatus(new Path(this.resDir + "/*"), FileUtils.HIDDEN_FILES_PATH_FILTER);
                    this.resDirPaths = new Path[resDirFS.length];
                    int pos = 0;
                    FileStatus[] arr$ = resDirFS;
                    int len$ = resDirFS.length;

                    for(int i$ = 0; i$ < len$; ++i$) {
                        FileStatus resFS = arr$[i$];
                        if (!resFS.isDir()) {
                            this.resDirPaths[pos++] = resFS.getPath();
                        }
                    }

                    if (pos == 0) {
                        return null;
                    } else {
                        return this.resFs.open(this.resDirPaths[this.resDirFilesNum++]);
                    }
                }
            } else {
                return this.getNextStream();
            }
        } catch (FileNotFoundException var8) {
            LOG.info("getStream error: " + StringUtils.stringifyException(var8));
            return null;
        } catch (IOException var9) {
            LOG.info("getStream error: " + StringUtils.stringifyException(var9));
            return null;
        }
    }

    private DataInput getNextStream() {
        try {
            return this.resDir != null && this.resDirFilesNum < this.resDirPaths.length && this.resDirPaths[this.resDirFilesNum] != null ? this.resFs.open(this.resDirPaths[this.resDirFilesNum++]) : null;
        } catch (FileNotFoundException var2) {
            LOG.info("getNextStream error: " + StringUtils.stringifyException(var2));
            return null;
        } catch (IOException var3) {
            LOG.info("getNextStream error: " + StringUtils.stringifyException(var3));
            return null;
        }
    }

    public void resetStream() {
        if (this.initialized) {
            this.resDirFilesNum = 0;
            this.initialized = false;
        }

    }

    private static boolean strEquals(String str1, String str2) {
        return org.apache.commons.lang.StringUtils.equals(str1, str2);
    }

    public void setTokenRewriteStream(TokenRewriteStream tokenRewriteStream) {
        assert this.tokenRewriteStream == null;

        this.tokenRewriteStream = tokenRewriteStream;
    }

    public TokenRewriteStream getTokenRewriteStream() {
        return this.tokenRewriteStream;
    }

    public static String generateExecutionId() {
        Random rand = new Random();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss_SSS");
        String executionId = "hive_" + format.format(new Date()) + "_" + Math.abs(rand.nextLong());
        return executionId;
    }

    public boolean isLocalOnlyExecutionMode() {
        return HiveConf.getVar(this.conf, ConfVars.HIVE_EXECUTION_ENGINE).equals("spark") ? false : ShimLoader.getHadoopShims().isLocalMode(this.conf);
    }

    public List<HiveLock> getHiveLocks() {
        return this.hiveLocks;
    }

    public void setHiveLocks(List<HiveLock> hiveLocks) {
        this.hiveLocks = hiveLocks;
    }

    public HiveTxnManager getHiveTxnManager() {
        return this.hiveTxnManager;
    }

    public void setHiveTxnManager(HiveTxnManager txnMgr) {
        this.hiveTxnManager = txnMgr;
    }

    public void setOriginalTracker(String originalTracker) {
        this.originalTracker = originalTracker;
    }

    public void restoreOriginalTracker() {
        if (this.originalTracker != null) {
            ShimLoader.getHadoopShims().setJobLauncherRpcAddress(this.conf, this.originalTracker);
            this.originalTracker = null;
        }

    }

    public void addCS(String path, ContentSummary cs) {
        this.pathToCS.put(path, cs);
    }

    public ContentSummary getCS(Path path) {
        return this.getCS(path.toString());
    }

    public ContentSummary getCS(String path) {
        return (ContentSummary)this.pathToCS.get(path);
    }

    public Map<String, ContentSummary> getPathToCS() {
        return this.pathToCS;
    }

    public Configuration getConf() {
        return this.conf;
    }

    public boolean isHDFSCleanup() {
        return this.isHDFSCleanup;
    }

    public void setHDFSCleanup(boolean isHDFSCleanup) {
        this.isHDFSCleanup = isHDFSCleanup;
    }

    public boolean isNeedLockMgr() {
        return this.needLockMgr;
    }

    public void setNeedLockMgr(boolean needLockMgr) {
        this.needLockMgr = needLockMgr;
    }

    public int getTryCount() {
        return this.tryCount;
    }

    public void setTryCount(int tryCount) {
        this.tryCount = tryCount;
    }

    public void setAcidOperation(Operation op) {
        this.acidOperation = op;
    }

    public Operation getAcidOperation() {
        return this.acidOperation;
    }
}
