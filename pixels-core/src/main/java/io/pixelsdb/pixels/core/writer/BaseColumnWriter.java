/*
 * Copyright 2017-2019 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.core.writer;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.Encoder;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.stats.StatsRecorder;
import io.pixelsdb.pixels.core.utils.BitUtils;
import io.pixelsdb.pixels.core.vector.ColumnVector;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteOrder;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * pixels
 *
 * @author guodong, hank
 * @create 2017-08-09
 * @update 2023-08-16 Chamonix: support nulls padding
 * @update 2023-08-27 Beijing: support isNull alignment
 */
public abstract class BaseColumnWriter implements ColumnWriter
{
    /**
     * The number of bytes that the start offset of the isNull bitmap is aligned to.
     */
    private static final int ISNULL_ALIGNMENT;
    /**
     * The byte buffer padded before the isNull bitmap for alignment.
     */
    private static final byte[] ISNULL_PADDING_BUFFER;

    static
    {
        ISNULL_ALIGNMENT = Integer.parseInt(ConfigFactory.Instance().getProperty("isnull.bitmap.alignment"));
        checkArgument(ISNULL_ALIGNMENT >= 0, "isnull.bitmap.alignment must >= 0");
        ISNULL_PADDING_BUFFER = new byte[ISNULL_ALIGNMENT];
    }

    final int pixelStride;                     // indicate num of elements in a pixel
    final EncodingLevel encodingLevel;         // indicate the encoding level during writing
    final ByteOrder byteOrder;                 // indicate the endianness used during writing
    final boolean nullsPadding;                // indicate whether nulls are padded by arbitrary value
    final boolean[] isNull;
    private final PixelsProto.ColumnChunkIndex.Builder columnChunkIndex;
    private final PixelsProto.ColumnStatistic.Builder columnChunkStat;

    final StatsRecorder pixelStatRecorder;
    final TypeDescription type;
    private final StatsRecorder columnChunkStatRecorder;

    private int lastPixelPosition = 0;                 // ending offset of last pixel in the column chunk
    private int curPixelPosition = 0;                  // current offset of this pixel in the column chunk. this is a relative value inside each column chunk.

    int curPixelEleIndex = 0;                  // index of elements in previous vector
    int curPixelVectorIndex = 0;               // index of the element to write in the current vector
    int curPixelIsNullIndex = 0;               // index of isNull in previous vector

    Encoder encoder;
    boolean hasNull = false;

    final ByteArrayOutputStream outputStream;  // column chunk content
    private final ByteArrayOutputStream isNullStream;  // column chunk isNull

    public BaseColumnWriter(TypeDescription type, PixelsWriterOption writerOption)
    {
        this.type = requireNonNull(type, "type is null");
        this.pixelStride = requireNonNull(writerOption, "writerOption is null").getPixelStride();
        this.encodingLevel = requireNonNull(writerOption.getEncodingLevel(), "encodingLevel is null");
        this.byteOrder = requireNonNull(writerOption.getByteOrder(), "byteOrder is null");
        this.nullsPadding = decideNullsPadding(writerOption);
        this.isNull = new boolean[pixelStride];
        this.columnChunkIndex = PixelsProto.ColumnChunkIndex.newBuilder()
                .setLittleEndian(byteOrder.equals(ByteOrder.LITTLE_ENDIAN))
                .setNullsPadding(nullsPadding)
                .setIsNullAlignment(ISNULL_ALIGNMENT);
        this.columnChunkStat = PixelsProto.ColumnStatistic.newBuilder();
        this.pixelStatRecorder = StatsRecorder.create(type);
        this.columnChunkStatRecorder = StatsRecorder.create(type);

        // TODO: a good estimation of chunk size is needed as the initial size of output stream
        this.outputStream = new ByteArrayOutputStream(pixelStride);
        this.isNullStream = new ByteArrayOutputStream(pixelStride);
    }

    @Override
    public abstract boolean decideNullsPadding(PixelsWriterOption writerOption);

    /**
     * Write ColumnVector
     * <p>
     * Serialize vector into {@code ByteBufferOutputStream}.
     * Update pixel statistics and positions.
     * Update column chunk statistics.
     *
     * @param vector vector
     * @param size   size of vector
     * @return size in bytes of the current column chunk
     */
    @Override
    public abstract int write(ColumnVector vector, int size) throws IOException;

    /**
     * Get byte array of column chunk content
     */
    @Override
    public byte[] getColumnChunkContent()
    {
        return outputStream.toByteArray();
    }

    /**
     * Get column chunk size in bytes
     */
    public int getColumnChunkSize()
    {
        return outputStream.size();
    }

    public PixelsProto.ColumnChunkIndex.Builder getColumnChunkIndex()
    {
        return columnChunkIndex;
    }

    public PixelsProto.ColumnStatistic.Builder getColumnChunkStat()
    {
        return columnChunkStatRecorder.serialize();
    }

    public StatsRecorder getColumnChunkStatRecorder()
    {
        return columnChunkStatRecorder;
    }

    public PixelsProto.ColumnEncoding.Builder getColumnChunkEncoding()
    {
        return PixelsProto.ColumnEncoding.newBuilder().setKind(PixelsProto.ColumnEncoding.Kind.NONE);
    }

    @Override
    public void flush() throws IOException
    {
        if (curPixelEleIndex > 0)
        {
            newPixel();
        }
        int isNullOffset = outputStream.size();
        if (ISNULL_ALIGNMENT != 0 && isNullOffset % ISNULL_ALIGNMENT != 0)
        {
            // Issue #548: align the start offset of the isNull bitmap for the compatibility of DuckDB
            int alignBytes =  ISNULL_ALIGNMENT - isNullOffset % ISNULL_ALIGNMENT;
            outputStream.write(ISNULL_PADDING_BUFFER, 0, alignBytes);
            isNullOffset += alignBytes;
        }
        // record isNull offset in the column chunk
        columnChunkIndex.setIsNullOffset(isNullOffset);
        // flush out isNullStream
        isNullStream.writeTo(outputStream);
    }

    void newPixel() throws IOException
    {
        // isNull
        if (hasNull)
        {
            isNullStream.write(BitUtils.bitWiseCompact(isNull, curPixelIsNullIndex, byteOrder));
            pixelStatRecorder.setHasNull();
        }
        // update position of current pixel
        curPixelPosition = outputStream.size();
        // set current pixel element count to 0 for the next batch pixel writing
        curPixelEleIndex = 0;
        curPixelVectorIndex = 0;
        curPixelIsNullIndex = 0;
        // update column chunk stat
        columnChunkStatRecorder.merge(pixelStatRecorder);
        // add current pixel stat and position info to columnChunkIndex
        PixelsProto.PixelStatistic.Builder pixelStat =
                PixelsProto.PixelStatistic.newBuilder();
        pixelStat.setStatistic(pixelStatRecorder.serialize());
        columnChunkIndex.addPixelPositions(lastPixelPosition);
        columnChunkIndex.addPixelStatistics(pixelStat.build());
        // update lastPixelPosition to current one
        lastPixelPosition = curPixelPosition;
        // reset current pixel stat recorder
        pixelStatRecorder.reset();
        // reset hasNull
        hasNull = false;
    }

    @Override
    public void reset()
    {
        lastPixelPosition = 0;
        curPixelPosition = 0;
        columnChunkIndex.clear();
        columnChunkStat.clear();
        pixelStatRecorder.reset();
        columnChunkStatRecorder.reset();
        outputStream.reset();
        isNullStream.reset();
    }

    @Override
    public void close() throws IOException
    {
        outputStream.close();
        isNullStream.close();
    }
}