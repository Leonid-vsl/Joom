package ru.org.joom;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by leonid on 03.04.2018.
 */
public class FileSorter {

    private static final Logger logger = Logger.getLogger(FileGenerator.class.getName());

    private static final int BATCH_SIZE = 5000;

    public static void main(String[] args) throws InterruptedException {


        try (RandomAccessFile fileReader = new RandomAccessFile(new File(args[0]), "rwd")) {
            String line;
            long startOffset = 0;
            StringBuffer buffer = new StringBuffer();
            long cnt = 0;
            while ((line = fileReader.readLine()) != null) {


                char[] chars = line.toCharArray();
                Arrays.sort(chars);
                buffer.append(chars);
                buffer.append(System.lineSeparator().toCharArray());

                if (cnt == BATCH_SIZE) {
                    fileReader.seek(startOffset);
                    fileReader.write(buffer.toString().getBytes());

                    buffer = new StringBuffer();
                    startOffset = fileReader.getFilePointer();
                    cnt = 0;
                }
                cnt++;

            }
            fileReader.seek(startOffset);
            fileReader.write(buffer.toString().getBytes());

        } catch (Exception e) {
            logger.log(Level.SEVERE, "error", e);
        }

        logger.log(Level.INFO, "File sorting complete");

    }
}
