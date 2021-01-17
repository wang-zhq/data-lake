// Main.java
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.io.*;

// 数据池的基本信息
class PoolBrief {
    String poolPath;
    HashMap<String, String> dirMap;
    int dataSize;
    int numOfCols;
    int[] sepIndex;
}

// 匹配参数要素集合
class FiltCond {
    String keyWord;
    String operator;
    String optCont;
    String optMode;
    String[] matchPatt;
}

public class Main {
    // 用户名称字符长度
    public static final String dbDirName = "../database";
    public static final String pfileName = "../params.txt";
    public static final String outfileName = "../out.txt";
    // 根据目录database的位置不同进行设置
    public static final int databasedirloc = 2;
    
    public static void main (String[] args) throws InterruptedException, IOException {
        // 计时开始
        long startTime = System.currentTimeMillis();
        // 建立线程池
        ExecutorService service = Executors.newCachedThreadPool();
        // 最后一级子目录数
        int subdirNum = countSubInfo(dbDirName);

        // 对最后一级子目录及其文件建索引
        PoolBrief[] dataBriefs = new PoolBrief[subdirNum];
        subdirNum = putDataInArray(dbDirName, dataBriefs, 0);
        System.out.println("Basic information complete. Elapse " + (System.currentTimeMillis()-startTime) + "ms");

        AtomicInteger genBriefChk = new AtomicInteger();
        HashMap<String, byte[]> dataBuff = new HashMap<String, byte[]> ();

        // 读入参数文件
        List<String> paramLines = Files.readAllLines(Paths.get(pfileName));
        int pNum = paramLines.size();

        // 开始根据参数分析文件，当第一级参数切换时，文件内容进行切换
        int[] matchCount = new int[pNum*2];
        AtomicIntegerArray atmMatchCount = new AtomicIntegerArray(matchCount);

        String tableLast = "";
        int p = 0;
        int i = 0;
        
        for (p = 0; p < pNum; p++) {
            
            String[] psec = paramLines.get(p).split(" ");
            // 如果两条参数的表名不同，则等待所有的线程结束
            if (! psec[0].equals(tableLast)) {
                for (i = 0; i < p; i++) {
                    while (atmMatchCount.get(i+pNum) < subdirNum) {
                        Thread.sleep(5);
                    }
                }
                // 替换上一条参数的表格名
                tableLast = psec[0];
                dataBuff.clear();

                genBriefChk.getAndSet(0);

                for (i = 0; i < subdirNum; i++) {
                    service.submit(new GenerateBriefsTASK(dataBriefs, dataBuff, i, genBriefChk, psec[0]));
                }

                while (genBriefChk.get() < subdirNum) {
                    Thread.sleep(5);
                }

            }

            FiltCond[] paramGroup = paramAnalyze(psec);
            FiltCond pathCdtn = paramGroup[0];
            FiltCond firstCon = paramGroup[1];
            FiltCond secondCon = paramGroup[2];

            String pathPatt = "";
            if (pathCdtn == null) {
                pathPatt = "%/" + psec[0] + "%";
            } else {
                pathPatt = "%/" + psec[0] + "/%" + pathCdtn.keyWord + "=%";
            }
            byte[] pathPattBytes = pathPatt.getBytes();

            for (i = 0; i < subdirNum; i++) {
                PoolBrief abrief = dataBriefs[i];

                String ipath = abrief.poolPath;
                if (! matchEngine(pathPattBytes, ipath.getBytes(), 0, ipath.length())) {
                    atmMatchCount.getAndIncrement(p+pNum);
                    continue;
                }

                if (pathCdtn != null) {
                    String objcon = abrief.dirMap.get(pathCdtn.keyWord);
                    if ( ! segMatch(objcon.getBytes(), 0, objcon.length(), pathCdtn) ) {
                        atmMatchCount.getAndIncrement(p+pNum);
                        continue;
                    }
                }

                int isize = abrief.dataSize;
                if (! dataBuff.containsKey(ipath)) {
                    // 如果缓存容量超过限额则清空
                    dataBuff.put(ipath, readFilesInOneDir(ipath, isize));
                }

                service.submit(new ParamLineMatchTASK(abrief, dataBuff.get(ipath), firstCon, secondCon, atmMatchCount, pNum, p));
            }
        }
        service.shutdown();

        for (p = 0; p < pNum; p++) {
            while (atmMatchCount.get(p+pNum) < subdirNum) {
                Thread.sleep(5);
            }
        }
        // while (atmMatchCount.get(2*pNum-1) < subdirNum) {
        //     Thread.sleep(2);
        // }

        File fout = new File(outfileName);
        if (fout.exists()) {
            fout.delete();
        }
        fout.createNewFile();
        
        FileWriter fw = new FileWriter(fout);
        BufferedWriter bw = new BufferedWriter(fw);
        
        for (i = 0; i < (pNum-1); i++) {
            bw.write(atmMatchCount.get(i) +"\n" );
        }
        bw.write(atmMatchCount.get(pNum-1)+"");
        bw.close(); 
        fw.close();

        System.out.println("ALL DONE. Elapse " + (System.currentTimeMillis()-startTime) + "ms");
    }

    // 将一个文件夹下的内容全读到数组里
    static byte[] readFilesInOneDir(String subpath, int dtLen) {
        byte[] contInPool = new byte[dtLen];
        int btst = 0;
        File ipath = new File(subpath);
        for (String con: ipath.list()) {
            if (con.charAt(0) == '.') {
                continue;
            }

            try {
                File tfile = new File(subpath, con);
                int tl = (int)tfile.length();

                FileInputStream cfin = new FileInputStream(tfile);
                cfin.read(contInPool, btst, tl);
                cfin.close();

                // Path cfPath = Paths.get(subpath, con);
                // FileChannel fc = FileChannel.open(cfPath, StandardOpenOption.READ);

                // MappedByteBuffer mbuf = fc.map(FileChannel.MapMode.READ_ONLY, 0, tl);
                // mbuf.get(contInPool, btst, tl);
                // fc.close();

                btst += tl;
            } catch (IOException e) {
                e.printStackTrace();
            }            

        }
        return contInPool;
    }
    
    // 分割匹配模板
    static String[] preparePatts(String s) {

        String[] dsp = s.split(",");
        String dsi = "";
        for (int i = 0; i < dsp.length; i++) {
            dsi = dsp[i];
            dsp[i] = dsi.substring(1, dsi.length()-1);
        }

        return dsp;
    }   
    
    // 字符串匹配
    static boolean segMatch(byte[] dtContent, int btStart, int btEnd, FiltCond fltCon) {
        if (fltCon.optMode.equals("V")) {
            int v = compareEngine(fltCon.optCont.getBytes(), dtContent, btStart, btEnd);
            String pop = fltCon.operator;

            if ( pop.equals("=") ) {
                return (v == 0);
            } else if ( pop.equals(">") ) {
                return (v > 0);
            } else if ( pop.equals("<") ) {
                return (v < 0);
            } else {
                return (v != 0);
            }

        } else {
            if (fltCon.operator.equals("ANY_LIKE")) {
                for (String p: fltCon.matchPatt) {
                    if (matchEngine(p.getBytes(), dtContent, btStart, btEnd)) {
                        return true;    
                    } 
                }
                return false;
            } else if ( fltCon.operator.equals("NONE_LIKE") ) {
                for (String p: fltCon.matchPatt) {
                    if (matchEngine(p.getBytes(), dtContent, btStart, btEnd)) {
                        return false;    
                    } 
                }
                return true;
            } else {
                for (String p: fltCon.matchPatt) {
                    if (! matchEngine(p.getBytes(), dtContent, btStart, btEnd)) {
                        return false;    
                    } 
                }
                return true;
            }
        }
    }

    // 统计数据库的基本信息
    static int countSubInfo(String dirname) {
        File currentPath = new File(dirname);
        int countInfo = 0;

        String[] conList = currentPath.list();
        String conpath = "";

        for (String con: conList) {
            if (con.charAt(0) == '.') {
                 continue;
            }               

            conpath = dirname + "/" + con;
            currentPath = new File(conpath);    
            if (currentPath.isDirectory()) {
                countInfo += countSubInfo (conpath);
            } else {
                countInfo = 1;
            }
        }
        return countInfo;
    }

    // 将最低一级目录的相关信息放入列表
    static int putDataInArray(String dirpath, PoolBrief[] dtlist, int rowst) {
        File currentPath = new File(dirpath);
        PoolBrief item = new PoolBrief();

        item.poolPath = dirpath;
        String[] conList = currentPath.list();
        int conlen = 0;

        for ( String con: conList ) {
            if (con.charAt(0) == '.') {
                continue;
            } 

            String conpath = dirpath + "/" + con;
            currentPath = new File(conpath);    
            if (currentPath.isDirectory()) {
                rowst = putDataInArray(conpath, dtlist, rowst);
            } else {
                conlen += (int)currentPath.length();
            }
        }
        
        if (conlen > 0) {
            item.dataSize = conlen;
            dtlist[rowst++] = item;
        }
        
        return rowst;
    }

    // 字节数组的匹配算法
    static boolean matchEngine(byte[] matchMould, byte[] matchObject, int btStart, int btEnd) {
        int mmLen = matchMould.length;
        int z = btStart;
        int retreatLoc = -1;

        int i = 0;
        while ( (i < mmLen) && (z < btEnd) ) {
            // 对%进行标记更新
            if (matchMould[i] == '%') {
                i++;
                retreatLoc = i;
                continue;
            } else if (matchMould[i] == '_') {
                i++;
                z++;
                continue;
            } else if (matchMould[i] == '\\') {
                i++;
            }

            if (matchMould[i] == matchObject[z]) {
                i++;
                z++;
            } else {
                if (retreatLoc == -1) {
                    break;
                } else if (retreatLoc < i) {
                    if ((retreatLoc == (i-1)) && (matchMould[retreatLoc] == '\\')) {
                        z++;
                    }
                    i = retreatLoc;
                } else {
                    z++;
                }
            }
        }

        if ((i == mmLen) && (z == btEnd)) {
            return true;
        } else if (z == btEnd) {
            if (i == (mmLen - 1)) {
                return (matchMould[i] == '%');
            }
        } else if (i == mmLen) {
            if (mmLen > 1) {
                if ((matchMould[mmLen-1] == '%') && (matchMould[mmLen-2] != '\\')) {
                    return true;

                } else {
                    // 反向匹配非通配符结尾的数据
                    int zRev = btEnd - 1;
                    for (int iRev = mmLen-1; iRev >= retreatLoc; iRev--) {
                        if (matchMould[iRev] != '\\') {
                            // 检测 “_” 但不是 “\_”的情况
                            if ((matchMould[iRev] == '_') && (matchMould[iRev-1] != '\\')) {
                                zRev --;
                                
                            } else {
                                if (matchMould[iRev] != matchObject[zRev]) {
                                    return false;
                                } else {
                                    zRev--;
                                }
                            }
                        } else {
                            if (matchMould[iRev-1] == '\\') {
                                if (matchObject[zRev] != '\\') {
                                    return false;
                                } else {
                                    zRev--;
                                }
                            }
                        }
                    }
                    return true;
                }
                
            } else {
                return (matchMould[0] == '%');
            }
        }
        return false;
    }

    // 字节数组的比较算法
    static int compareEngine(byte[] matchMould, byte[] matchObject, int btStart, int btEnd) {
        int mmLen = matchMould.length;
        int z = btStart;

        int i = 0;
        while ( (i < mmLen) && (z < btEnd) ) {

            if (matchObject[z] > matchMould[i]) {
                return 1;
            } else if (matchObject[z] < matchMould[i]) {
                return -1;
            }
            i++;
            z++;
        }

        if (z < btEnd) {
            return 1;
        } else if (i < mmLen) {
            return -1;
        }

        return 0;
    }

    // 参数分析，按两组参数的模式进行，但有时参数是路径的参数
    static FiltCond[] paramAnalyze(String[] psec) {

        FiltCond firstCon = new FiltCond();
        FiltCond secondCon = new FiltCond();

        String optmod = "%LIKE";
        
        firstCon.keyWord = psec[1];
        firstCon.operator = psec[2];
        firstCon.optCont = psec[3].substring(1, psec[3].length()-1);
        firstCon.optMode = ( matchEngine(optmod.getBytes(), psec[2].getBytes(), 0, psec[2].length()) ) ? "M" : "V";
        if (firstCon.optMode.equals("M")) {
            firstCon.matchPatt = Main.preparePatts(firstCon.optCont);
        }

        secondCon.keyWord = psec[4];
        secondCon.operator = psec[5];
        secondCon.optCont = psec[6].substring(1, psec[6].length()-1);
        secondCon.optMode = ( matchEngine(optmod.getBytes(), psec[5].getBytes(),0, psec[5].length()) ) ? "M" : "V";
        if (secondCon.optMode.equals("M")) {
            secondCon.matchPatt = Main.preparePatts(secondCon.optCont);
        }

        FiltCond[] paramGroup = new FiltCond[3];
        String colmod = "column%";
        if ( matchEngine(colmod.getBytes(), psec[1].getBytes(), 0, psec[1].length()) ) {
            if ( matchEngine(colmod.getBytes(), psec[4].getBytes(), 0, psec[4].length()) ) {
                paramGroup[0] = null;
                paramGroup[1] = firstCon;
                paramGroup[2] = secondCon;
            } else {
                paramGroup[0] = secondCon;
                paramGroup[1] = firstCon;
                paramGroup[2] = null;
            }
        } else {
            paramGroup[0] = firstCon;
            paramGroup[1] = secondCon;
            paramGroup[2] = null;
        }

        String kwd = paramGroup[1].keyWord;
        paramGroup[1].keyWord = kwd.substring(6,kwd.length());

        if (paramGroup[2] != null) {
            kwd = paramGroup[2].keyWord;
            paramGroup[2].keyWord = kwd.substring(6,kwd.length());
        }
        
        return paramGroup;
    }
}

// 生成子目录数据
class GenerateBriefsTASK implements Runnable {

    private PoolBrief[] dataBriefs;
    private HashMap<String, byte[]> dataBuff;
    private int Idx;
    private AtomicInteger genBriefChk;
    private String tableName;

    public GenerateBriefsTASK(PoolBrief[] dbfs, HashMap<String, byte[]> dbuff, int id, AtomicInteger gbc, String tbl) {
        this.dataBriefs = dbfs;
        this.dataBuff = dbuff;
        this.Idx = id;
        this.genBriefChk = gbc;
        this.tableName = tbl;
    }

    @Override
    public void run() {
        String subpath = dataBriefs[Idx].poolPath;
        int dtLen = dataBriefs[Idx].dataSize;

        // 将文件路径拆分成map形式，以利于检索
        String[] pathsec = subpath.split("/");
        if (pathsec[Main.databasedirloc].equals(tableName)) {
            
            byte[] contInPool = Main.readFilesInOneDir(subpath, dtLen);

            if (dataBriefs[Idx].dirMap == null) {
                HashMap<String, String> pathmap = new HashMap<String, String>();
                // table 的路径位置视情况而定 mvn时是2
                // int databasedirloc = 1;
                pathmap.put("table", tableName);
                for (int p = (Main.databasedirloc+1); p < pathsec.length; p++) {
                    String[] psp = pathsec[p].split("=");
                    pathmap.put(psp[0], psp[1]);
                }
        
                int btsinrow = 0;
                int segsinrow = 0;
                while (contInPool[btsinrow] != '\n') {
                    if (contInPool[btsinrow] == '|') {
                        segsinrow++;
                    }
                    btsinrow++;
                }
        
                int cntrow = 0;
                btsinrow = 0;
                while (cntrow < 5) {
                    if (contInPool[btsinrow] == '\n') {
                        cntrow++;
                    }
                    btsinrow++;
                }
                btsinrow /= cntrow;
        
                dataBriefs[Idx].numOfCols = segsinrow + 2;
                int[] seglocs = new int[dtLen/btsinrow*(segsinrow+2)*3];
        
                seglocs[0] = -1;
                int loc = 1;
                for (int z = 0; z < dtLen; z++) {
                    if ((contInPool[z] == '|') || (contInPool[z] == '\r') || contInPool[z] == '\n') {
                        seglocs[loc] = z;
                        loc++;
                    }
                }
                
                int[] realsloc = new int[loc];
                System.arraycopy(seglocs, 0, realsloc, 0, loc);
                dataBriefs[Idx].sepIndex = realsloc;
        
                dataBriefs[Idx].dirMap = pathmap;
        
                seglocs = null;
                realsloc = null;
                pathmap = null;
            }

            dataBuff.put(subpath, contInPool);

            contInPool = null;
        }

        genBriefChk.getAndIncrement();
    }
}


// 线程匹配
class ParamLineMatchTASK implements Runnable {
    private PoolBrief poolInfo;
    private byte[] contInPool;
    private FiltCond firstCon;
    private FiltCond secondCon;
    private AtomicIntegerArray resVec;
    private int pNum;
    private int pId;

    public ParamLineMatchTASK(PoolBrief pinfo, byte[] contin, FiltCond fcon, FiltCond scon, AtomicIntegerArray stvec, int np, int p) {
        this.poolInfo = pinfo;
        this.contInPool = contin;
        this.firstCon = fcon;
        this.secondCon = scon;
        this.resVec = stvec;
        this.pNum = np;
        this.pId = p;
    }

    @Override
    public void run(){

        int[] colIdx = poolInfo.sepIndex;
        int colNum = poolInfo.numOfCols;
        int numOfSeg = poolInfo.sepIndex.length;

        int c1 = Integer.parseInt(firstCon.keyWord);
        int c2 = (secondCon == null) ? (-1) : Integer.parseInt(secondCon.keyWord);

        int cid = 0;
        int matchtimes = 0;
        for (int i = 0; i < (numOfSeg/colNum); i++) {
            cid = i*colNum + c1;
            if (! Main.segMatch(contInPool, colIdx[cid]+1, colIdx[cid+1], firstCon)) {
                continue;
            }

            if (c2 >= 0) {
                cid = i*colNum + c2;
                if (! Main.segMatch(contInPool, colIdx[cid]+1, colIdx[cid+1], secondCon)) {
                    continue;
                }
            }
            matchtimes++;
        }

        resVec.getAndAdd(pId, matchtimes);
        resVec.getAndIncrement(pId+pNum);
    }
}
