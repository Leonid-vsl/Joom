package ru.org.joom;

import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by leonid on 03.04.2018.
 */
public class FileGenerator {

    private static final Logger logger = Logger.getLogger(FileGenerator.class.getName());

    public static void generate(String fileName, int rowNum, int rowLength) {


        try (PrintWriter out = new PrintWriter(fileName)) {
            for (int i = 0; i < rowNum; i++) {
                out.println(generateStr(rowLength));
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error while write to file.", e);
        }
        logger.log(Level.INFO, "File generation complete");

    }

    private static String generateStr(int length) {

        StringBuilder builder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            builder.append((char)('A' + Math.random() * ('Z' - 'A')));
        }
        return builder.toString();
    }

    public static void main(String[] args) {
        generate("test.txt",100000,20);
    }
}
