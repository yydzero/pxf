package org.greenplum.pxf.automation.features.image;

import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class HdfsReadableImageTest extends BaseFeature {
    private String hdfsPath;
    BufferedImage[] bufferedImages;
    File[] filesToDelete;
    private static int w = 256;
    private static int h = 128;
    private String publicStage = "/tmp/publicstage/pxf";
    private File publicStageDir;
    private StringBuilder[] images;

    private Table compareTable;

    @Override
    public void beforeClass() throws Exception {
        super.beforeClass();
        // path for storing data on HDFS (for processing by PXF)
        hdfsPath = hdfs.getWorkingDirectory() + "/readableImage";
        prepareData();
    }

    private void prepareData() throws Exception {
        bufferedImages = new BufferedImage[5];
        filesToDelete = new File[5];
        for (int i = 0; i < bufferedImages.length; i++) {
            bufferedImages[i] = new BufferedImage(w, h, BufferedImage.TYPE_INT_RGB);
        }
        images = new StringBuilder[5];
        appendToImages(images, "{{", 0);

        Map<String, Integer> maskOffsets = new HashMap<String, Integer>() {{
            put("r", 16);
            put("g", 8);
            put("b", 0);
        }};

        Random rand = new Random();
        for (int j = 0; j < bufferedImages.length; j++) {
            for (int i = 0; i < w * h; i++) {
                final int r = rand.nextInt(255) << maskOffsets.get("r");
                final int g = rand.nextInt(255) << maskOffsets.get("g");
                final int b = rand.nextInt(255) << maskOffsets.get("b");
                bufferedImages[j].setRGB(i % w, i / w, r + g + b);
                images[j].append("{").append(r >> maskOffsets.get("r")).append(",").append(g >> maskOffsets.get("g")).append(",").append(b >> maskOffsets.get("b")).append("},");
                if ((i + 1) % w == 0) {
                    appendToImage(images[j], "},{", 1);
                }
            }
        }

        appendToImages(images, "}", 2);
        publicStageDir = new File(publicStage);
        if (!publicStageDir.exists()) {
            if (!publicStageDir.mkdirs()) {
                throw new RuntimeException(String.format("Could not create %s", publicStage));
            }

        }
        int cnt = 0;
        for (BufferedImage bi : bufferedImages) {
            final String fileName = String.format("%d.png", cnt);
            filesToDelete[cnt] = new File(publicStage + "/" + fileName);
            ImageIO.write(bi, "png", filesToDelete[cnt]);
            hdfs.copyFromLocal(filesToDelete[cnt].toString(), hdfsPath + "/" + fileName);
            cnt++;
        }
    }

    private void appendToImages(StringBuilder[] sbs, String s, int offsetFromEnd) {
        if (sbs[0] == null) {
            for (int i = 0; i < sbs.length; i++) {
                sbs[i] = new StringBuilder();
            }
        }
        for (StringBuilder sb : sbs) {
            appendToImage(sb, s, offsetFromEnd);
        }
    }

    private void appendToImage(StringBuilder sb, String s, int offsetFromEnd) {
        if (sb == null) {
            sb = new StringBuilder();
        }
        sb.setLength(sb.length() - offsetFromEnd);
        sb.append(s);
    }

    @Override
    public void beforeMethod() throws Exception {
        // default external table with common settings
        exTable = new ReadableExternalTable("image_test", null, "", "CSV");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setFields(new String[]{"fullpaths TEXT[]", "directories TEXT[]", "names TEXT[]", "images INT[]"});
        exTable.setPath(hdfsPath + "/*.png");
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":image");
        compareTable = new Table("compare_table", new String[]{"id INT", "images INT[]"});
        compareTable.setDistributionFields(new String[]{"id"});
    }

    /**
     * Read a single image from HDFS
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void singleImage() throws Exception {
        exTable.setName("image_test_batchsize_1");
        compareTable.setName("compare_table_batchsize_1");
        int cnt = 0;
        for (StringBuilder image : images) {
            compareTable.addRow(new String[]{String.valueOf(cnt++), "'{" + image + "}'"});
        }

        gpdb.createTableAndVerify(exTable);
        gpdb.createTableAndVerify(compareTable);
        gpdb.runQuery(compareTable.constructInsertStmt());

        // Verify results
        runTincTest("pxf.features.hdfs.readable.image.batchsize_1.runTest");
    }


    @Override
    public void afterClass() throws Exception {
        super.afterMethod();
        if (ProtocolUtils.getPxfTestDebug().equals("true")) {
            return;
        }
        for (File fileToDelete : filesToDelete) {
            if (!fileToDelete.delete()) {
                throw new RuntimeException(String.format("Could not delete %s", fileToDelete));
            }
        }

    }
}
