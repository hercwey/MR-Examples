package org.pungii.myzk.hadoop;

import hdfs.HdfsDAO;

import java.io.IOException;


public class Profit {

    public static void main(String[] args) throws Exception {
        profit();
    }

    public static void profit() throws Exception {
        String[] sellStr = getSell().split(",");
        //
        int sell = Integer.parseInt(sellStr[0]);
        String id = sellStr[1];
        
        int purchase = getPurchase();
        int other = getOther();
        int profit = sell - purchase - other;
        System.out.printf("profit = sell - purchase - other = %d - %d - %d = %d\n", sell, purchase, other, profit);
        System.out.println("product id: "+id);
    }

    public static int getPurchase() throws Exception {
        HdfsDAO hdfs = new HdfsDAO(Purchase.HDFS, Purchase.config());
        return Integer.parseInt(hdfs.cat(Purchase.path().get("output") + "/part-r-00000").trim());
    }

    public static String getSell() throws Exception {
        HdfsDAO hdfs = new HdfsDAO(Sell.HDFS, Sell.config());
        return hdfs.cat(Sell.path().get("output") + "/part-r-00000").trim();
    }

    public static int getOther() throws IOException {
        return Other.calcOther(Other.file);
    }

}
