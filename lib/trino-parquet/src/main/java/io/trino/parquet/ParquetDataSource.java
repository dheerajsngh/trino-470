/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.parquet;

import com.google.common.collect.ListMultimap;
import io.airlift.slice.Slice;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.reader.ChunkedInputStream;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

public interface ParquetDataSource
        extends Closeable
{
    ParquetDataSourceId getId();

    long getReadBytes();

    long getReadTimeNanos();

    long getEstimatedSize();

    Slice readTail(int length)
            throws IOException;

    Slice readFully(long position, int length)
            throws IOException;

    <K> Map<K, ChunkedInputStream> planRead(ListMultimap<K, DiskRange> diskRanges, AggregatedMemoryContext memoryContext);

    default java.util.List<java.util.concurrent.CompletableFuture<java.nio.ByteBuffer>> readVectored(java.util.List<io.trino.filesystem.FileRange> ranges, java.util.function.IntFunction<java.nio.ByteBuffer> allocator)
            throws IOException
    {
        throw new UnsupportedOperationException("readVectored is not implemented for this Parquet Data Source");
    }

    default void fetchRangesVectored(java.util.List<io.trino.filesystem.FileRange> ranges, io.trino.memory.context.LocalMemoryContext memoryContext)
            throws IOException
    {
        java.util.function.IntFunction<java.nio.ByteBuffer> trackedAllocator = size -> {
            memoryContext.setBytes(memoryContext.getBytes() + size);
            return java.nio.ByteBuffer.allocate(size);
        };
        readVectored(ranges, trackedAllocator);
    }

    @Override
    default void close()
            throws IOException
    {}
}
