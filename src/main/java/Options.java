import org.apache.hadoop.util.StringUtils;


public enum Options {
    DELETE("-delete", DistCPPlus.NAME + ".delete"),
    FILE_LIMIT("-filelimit", DistCPPlus.NAME + ".limit.file"),
    SIZE_LIMIT("-sizelimit", DistCPPlus.NAME + ".limit.size"),
    IGNORE_READ_FAILURES("-i", DistCPPlus.NAME + ".ignore.read.failures"),
    PRESERVE_STATUS("-p", DistCPPlus.NAME + ".preserve.status"),
    OVERWRITE("-overwrite", DistCPPlus.NAME + ".overwrite.always"),
    UPDATE("-update", DistCPPlus.NAME + ".overwrite.ifnewer"),
    SKIPCRC("-skipcrccheck", DistCPPlus.NAME + ".skip.crc.check"),
    SKIPTS("-skiptscheck", DistCPPlus.NAME + ".skip.ts.check"),
    SKIPUPDATECHECK("-skipupdatecheck", DistCPPlus.NAME+".skip.update.check");

    final String cmd, propertyname;

    private Options(String cmd, String propertyname) {
        this.cmd = cmd;
        this.propertyname = propertyname;
    }

    public long parseLong(String[] args, int offset) {
        if (offset ==  args.length) {
            throw new IllegalArgumentException("<n> not specified in " + cmd);
        }
        long n = StringUtils.TraditionalBinaryPrefix.string2long(args[offset]);
        if (n <= 0) {
            throw new IllegalArgumentException("n = " + n + " <= 0 in " + cmd);
        }
        return n;
    }

    public String getCommand() {
        return cmd;
    }
}