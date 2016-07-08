package com.log.process;

import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;

import java.io.*;
import java.util.*;

/**
 * Created by Administrator on 7/4/2016.
 */
public class LogConversion {
    //static variables for keeping track of count
    static int count = 0;
    static int rownum = 0;
    static int cellnum = 0;
    static Iterator val_iterator = null;
    public static void main(String[] args) throws IOException {
        //reading the input CSV File
        String csvFile = "D:\\input.csv";
        BufferedReader br = null;
        BufferedReader bufferedReader = null;
        String line = "";
        //space as delimiter
        String csvSplitBy = " ";
        //creating the workbook and worksheet
        HSSFWorkbook workbook = new HSSFWorkbook();
        HSSFSheet sheet = workbook.createSheet("Log Data");

        br = new BufferedReader(new FileReader(csvFile));
        bufferedReader = new BufferedReader(new FileReader(csvFile));
        Set<String> set  = new LinkedHashSet<>();
        //adding coloumns Date and Time manually
        set.add("Date");
        set.add("Time");
        //loop is for extracting all the possible keys in the input CSV File
        while ((line = br.readLine()) != null) {
            // use space as separator
            String[] split = line.split(csvSplitBy);
            for (String s : split) {
                String[] key_value = s.split("[=]");
                if (key_value.length>1) {
                    set.add(key_value[0]);
                }
            }
        }

        Iterator iterator = set.iterator();
        Cell cell = null;
        Row row = sheet.createRow(rownum++);
        //Setting all the possible keys as coloumn headers in the sheet
        while (iterator.hasNext()){
            String key = (String) iterator.next();
            cell = row.createCell(cellnum++);
            cell.setCellValue(key);
        }

        cellnum = 0;
        List<String> value = null;
        //removed Date and Time fields from the setting the values for the corresponding keys
        set.remove("Date");
        set.remove("Time");
        //converting the input key set to string array of keys
        String[] strArr = set.toArray(new String[set.size()]);
        //loop for adding the rows
        while ((line = bufferedReader.readLine()) != null)  {
            row = sheet.createRow(rownum++);
            String[] split = line.split(csvSplitBy);
            value = new LinkedList<>();
            value.add(split[1]);
            value.add(split[2]);
            count = 0;
            for (String s : split){
                //regex for splitting the values
                String[] key_value = s.split("[=]");
                if (key_value.length>1) {
                    while (count < strArr.length) {
                        if(key_value[0].equals(strArr[count])) {
                            value.add(key_value[1]);
                            count++;
                            break;
                        }
                        else
                            value.add("    ");
                        count++;
                    }
                }
            }
            val_iterator = value.iterator();
            while (val_iterator.hasNext()){
                String v = (String) val_iterator.next();
                cell = row.createCell(cellnum++);
                cell.setCellValue(String.valueOf(v));
            }
            cellnum = 0;
        }
        //writing the output in the result sheet
        try
        {
            //Write the workbook in file system
            FileOutputStream out = new FileOutputStream(new File("D:\\output.xls"));
            workbook.write(out);
            out.close();
            System.out.println("Successfully done....");
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

    }
}
