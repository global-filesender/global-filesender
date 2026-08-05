if (typeof window === 'undefined')
    window = {}; // dummy window for use in webworkers

if (!('filesender' in window))
    window.filesender = {};

/**
 * Reorder buffer for the parallel (TeraReceiver) download path.
 *
 * Parallel workers download and decrypt chunks out of order, but the download
 * sinks (legacy blob array, StreamSaver, FileSystemWritableFileStream) all
 * assume chunks arrive strictly in ascending order. This buffer queues
 * decrypted chunks keyed by their chunk id and flushes them to the sink in
 * order, so sink.visit() is always called in the exact same sequence as the
 * old single-worker path.
 *
 * It also exposes the in-order write pointer (nextToWrite) so the driver can
 * apply backpressure: never let workers download/buffer too far ahead of the
 * chunk currently being written.
 *
 * @param lastChunkId  zero-based id of the final chunk (encryption_details.chunkcount)
 * @param sink         object with async visit(chunkid, Uint8Array) and done()
 * @param onAllWritten called once, after every chunk 0..lastChunkId is written
 * @param onError      called once if a sink write throws
 */
window.filesender.crypto_reorder_buffer = function (lastChunkId, sink, onAllWritten, onError) {
    return {
        lastChunkId: lastChunkId,
        sink: sink,
        onAllWritten: onAllWritten,
        onError: onError,

        nextToWrite: 0,   // next chunk id that must be written to the sink
        ready: {},        // chunkid -> decrypted Uint8Array awaiting its turn
        writing: false,   // re-entrancy guard so only one drain loop runs
        aborted: false,   // an error has stopped the transfer
        finished: false,  // onAllWritten has already fired

        /**
         * Number of chunks downloaded/buffered but not yet written, given how
         * many chunks have been allocated so far. Used by the driver for
         * backpressure.
         */
        outstanding: function (allocatedCount) {
            return allocatedCount - this.nextToWrite;
        },

        /**
         * Stop accepting and writing chunks (called on a decrypt failure).
         */
        abort: function () {
            this.aborted = true;
        },

        /**
         * Queue a decrypted chunk and flush as far as possible, in order.
         * Returns the drain() promise so callers may await it in tests.
         */
        submit: function (chunkid, data) {
            if (this.aborted) return Promise.resolve();
            this.ready[chunkid] = data;
            return this.drain();
        },

        /**
         * Write every consecutive ready chunk starting at nextToWrite, then
         * fire onAllWritten once the final chunk has been written.
         */
        drain: async function () {
            var $this = this;
            if ($this.writing || $this.aborted) return;
            $this.writing = true;
            try {
                while (Object.prototype.hasOwnProperty.call($this.ready, $this.nextToWrite)) {
                    var id = $this.nextToWrite;
                    var data = $this.ready[id];
                    delete $this.ready[id];
                    await $this.sink.visit(id, data);
                    $this.nextToWrite = id + 1;
                }
            } catch (e) {
                $this.writing = false;
                if (!$this.aborted) {
                    $this.aborted = true;
                    $this.onError(e);
                }
                return;
            }
            $this.writing = false;

            if (!$this.finished && !$this.aborted &&
                $this.nextToWrite > $this.lastChunkId) {
                $this.finished = true;
                $this.onAllWritten();
            }
        }
    };
};
