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
package io.trino.filesystem.hdfs;

import io.airlift.slice.Slice;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.hdfs.FSDataInputStreamTail;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.FileNotFoundException;
import java.io.IOException;

import static io.trino.filesystem.hdfs.HdfsFileSystem.withCause;
import static java.util.Objects.requireNonNull;

class HdfsInput
        implements TrinoInput
{
    private static final io.airlift.log.Logger log = io.airlift.log.Logger.get(HdfsInput.class);

    private final FSDataInputStream stream;
    private final TrinoInputFile inputFile;
    private boolean closed;

    public HdfsInput(FSDataInputStream stream, TrinoInputFile inputFile)
    {
        this.stream = requireNonNull(stream, "stream is null");
        this.inputFile = requireNonNull(inputFile, "inputFile is null");
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        ensureOpen();
        try {
            stream.readFully(position, buffer, bufferOffset, bufferLength);
        }
        catch (FileNotFoundException e) {
            throw withCause(new FileNotFoundException("File %s not found: %s".formatted(toString(), e.getMessage())), e);
        }
        catch (IOException e) {
            throw new IOException("Read exactly %s bytes at position %s of file %s failed: %s".formatted(bufferLength, position, toString(), e.getMessage()), e);
        }
    }

    @Override
    public int readTail(byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        ensureOpen();
        try {
            Slice tail = FSDataInputStreamTail.readTail(toString(), inputFile.length(), stream, bufferLength).getTailSlice();
            tail.getBytes(0, buffer, bufferOffset, tail.length());
            return tail.length();
        }
        catch (FileNotFoundException e) {
            throw withCause(new FileNotFoundException("File %s not found: %s".formatted(toString(), e.getMessage())), e);
        }
        catch (IOException e) {
            throw new IOException("Read %s tail bytes of file %s failed: %s".formatted(bufferLength, toString(), e.getMessage()), e);
        }
    }

    @Override
    public void readVectored(java.util.List<io.trino.filesystem.FileRange> ranges, java.util.function.IntFunction<java.nio.ByteBuffer> allocator)
            throws IOException
    {
        ensureOpen();
        // log.info("VECTORED_IO_TRIGGERED: Dispatched parallelised GCS reads with %d ranges for file %s", ranges.size(), inputFile.location());
        // Translate Trino's format-neutral FileRange models to Hadoop FileRange implementations
        java.util.List<org.apache.hadoop.fs.FileRange> hadoopRanges = ranges.stream()
                .map(range -> org.apache.hadoop.fs.FileRange.createFileRange(
                        range.offset(),
                        range.length()))
                .collect(java.util.stream.Collectors.toList());

        // Delegate to the underlying Hadoop stream to trigger VectoredIOImpl
        try {
            stream.readVectored(hadoopRanges, allocator);
        }
        catch (FileNotFoundException e) {
            throw withCause(new FileNotFoundException("File %s not found: %s".formatted(toString(), e.getMessage())), e);
        }
        catch (IOException e) {
            throw new IOException("Read vectored of file %s failed: %s".formatted(toString(), e.getMessage()), e);
        }

        // Propagate the futures from Hadoop's FileRanges back to Trino's FileRanges
        for (int i = 0; i < ranges.size(); i++) {
            io.trino.filesystem.FileRange trinoRange = ranges.get(i);
            org.apache.hadoop.fs.FileRange hadoopRange = hadoopRanges.get(i);
            hadoopRange.getData().whenComplete((buffer, throwable) -> {
                if (throwable != null) {
                    trinoRange.data().completeExceptionally(throwable);
                }
                else {
                    trinoRange.data().complete(buffer);
                }
            });
        }

        // Block the main thread, waiting for all background GCS transfers to finish
        for (org.apache.hadoop.fs.FileRange hadoopRange : hadoopRanges) {
            try {
                hadoopRange.getData().get(); // Synchronous block until this segment is fully downloaded
            }
            catch (java.util.concurrent.ExecutionException | InterruptedException e) {
                throw new java.io.IOException("Vectored read failed asynchronously inside GCS connector", e);
            }
        }
    }

    @Override
    public void close()
            throws IOException
    {
        closed = true;
        stream.close();
    }

    @Override
    public String toString()
    {
        return inputFile.toString();
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Output stream closed: " + this);
        }
    }
}
