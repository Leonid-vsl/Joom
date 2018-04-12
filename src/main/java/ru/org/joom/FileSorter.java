package ru.org.joom;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;


public class FileSorter {

    private static final Logger logger = Logger.getLogger(FileGenerator.class.getName());

    private static final int BATCH_SIZE = 2;

    public static void main(String[] args) throws InterruptedException, IOException {

        ChunkInfo chunkInfo = sortInChunks(args[0]);

        NavigableMap<Long, Long> positions = getPositions(args[0], chunkInfo);

        swapPositions(args[0], chunkInfo, positions);

        logger.log(Level.INFO, "File sorting complete");

    }

    private static Map.Entry<Long, Long> findMin(NavigableMap<Long, Long> positions) {

        OptionalLong min = positions.values().stream().mapToLong(value -> value.longValue()).min();
        for (Map.Entry<Long, Long> position : positions.entrySet()) {
            if (position.getValue() == min.getAsLong()) {
                return position;
            }
        }
        return null;
    }

    private static void swapPositions(String arg, ChunkInfo chunkInfo, NavigableMap<Long, Long> positions) {
        try (RandomAccessFile fileReader = new RandomAccessFile(new File(arg), "rwd")) {

            fileReader.seek(0);

            String swap = null;

            Map.Entry<Long, Long> min = findMin(positions);
            Long fromPosition = min.getKey();


            while (positions.size() > 0) {

                Long toPosition = positions.get(fromPosition);
                if (toPosition == null) {
                    min = findMin(positions);
                    swap = null;
                    fromPosition = min.getKey();
                    continue;
                }
                positions.remove(fromPosition);

                String startRow = readByOffset(fileReader, fromPosition, chunkInfo.getChunkLen());
                String endRow = readByOffset(fileReader, toPosition, chunkInfo.getChunkLen());

                if (swap != null) {
                    writeByOffset(fileReader, toPosition, chunkInfo.getChunkLen(), swap);
                } else {
                    writeByOffset(fileReader, toPosition, chunkInfo.getChunkLen(), startRow);
                }

                fromPosition = toPosition;
                swap = endRow;

            }


        } catch (Exception e) {
            logger.log(Level.SEVERE, "error", e);
        }
    }

    private static NavigableMap<Long, Long> getPositions(String arg, ChunkInfo chunkInfo) throws IOException {
        int[] chunkOffsets = new int[(int) chunkInfo.chunkCount];

        NavigableMap<Long, Long> swapPositions = new TreeMap<>();

        try (RandomAccessFile fileReader = new RandomAccessFile(new File(arg), "rwd")) {

            long cnt = 0;

            while (Arrays.stream(chunkOffsets).sum() < chunkInfo.getRowsCnt()) {

                NavigableMap<String, Integer> merge = new TreeMap<>();

                for (int i = 0; i < chunkOffsets.length; i++) {

                    if (chunkOffsets[i] == BATCH_SIZE) continue;

                    fileReader.seek((i * BATCH_SIZE * (chunkInfo.getChunkLen() + 1)) + (chunkOffsets[i] * (chunkInfo.getChunkLen() + 1)));
                    String line = fileReader.readLine();
                    merge.putIfAbsent(line, i);

                }

                Map.Entry<String, Integer> lowerItem = merge.firstEntry();
                int chunkNumber = lowerItem.getValue();
                long newPos = (long) (chunkNumber * BATCH_SIZE + chunkOffsets[chunkNumber]);
                if (cnt != newPos) {
                    swapPositions.put(newPos, cnt);
                }

                chunkOffsets[chunkNumber]++;
                cnt++;

            }

        }
        return swapPositions;
    }

    private static String readByOffset(RandomAccessFile fileReader, Long position, Long rowLen) throws IOException {
        long offset = position * (rowLen + 1);
        fileReader.seek(offset);
        return fileReader.readLine() + System.lineSeparator();

    }

    private static void writeByOffset(RandomAccessFile fileReader, Long position, Long rowLen, String value) throws IOException {
        long offset = position * (rowLen + 1);
        fileReader.seek(offset);
        fileReader.write(value.getBytes());
    }

    private static ChunkInfo sortInChunks(String fileName) throws IOException {
        int chunks = 0;
        long lineLen = 0;

        try (RandomAccessFile fileReader = new RandomAccessFile(new File(fileName), "rwd")) {
            String line;
            long cnt = 0;


            TreeMap<String, Integer> sorted = new TreeMap<>();
            long startOffset = 0;
            StringBuffer buffer = new StringBuffer();

            while ((line = fileReader.readLine()) != null) {
                cnt++;

                if (lineLen == 0) lineLen = line.length();
                char[] chars = line.toCharArray();
                Arrays.sort(chars);
                String sortedLine = new String(chars);

                sorted.computeIfPresent(sortedLine, (s, counter) -> ++counter);
                sorted.putIfAbsent(sortedLine, 0);


                if (cnt == BATCH_SIZE) {

                    flushBuf(fileReader, sorted, startOffset, buffer);
                    buffer = new StringBuffer();
                    sorted.clear();
                    cnt = 0;

                    startOffset = (lineLen + 1) * ((chunks + 1) * BATCH_SIZE);
                    fileReader.seek(startOffset);
                    chunks++;
                }
            }

            flushBuf(fileReader, sorted, startOffset, buffer);
        }
        return new ChunkInfo(chunks, lineLen, chunks * BATCH_SIZE);
    }

    private static void flushBuf(RandomAccessFile fileReader, Map<String, Integer> sorted, long startOffset, StringBuffer buffer) throws IOException {
        for (String str : sorted.keySet()) {
            for (int i = 0; i <= sorted.get(str); i++) {
                buffer.append(str);
                buffer.append(System.lineSeparator().toCharArray());
            }
        }
        fileReader.seek(startOffset);
        fileReader.write(buffer.toString().getBytes());
    }

    private static class ChunkInfo {

        private final long chunkCount;
        private final long chunkLen;
        private final long rowsCnt;


        private ChunkInfo(long chunkCount, long chunkLen, long rowsCnt) {
            this.chunkCount = chunkCount;
            this.chunkLen = chunkLen;
            this.rowsCnt = rowsCnt;
        }

        public long getChunkCount() {
            return chunkCount;
        }

        public long getChunkLen() {
            return chunkLen;
        }

        public long getRowsCnt() {
            return rowsCnt;
        }
    }

}
