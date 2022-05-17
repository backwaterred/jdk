/*
 * Copyright (c) 2022, Oracle and/or its affiliates. All rights reserved.
 * Copyright (c) 2022, IBM Corp.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

package sun.nio.fs;

import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;
import java.io.IOException;
import jdk.internal.misc.Unsafe;

import static sun.nio.fs.UnixNativeDispatcher.*;
import static sun.nio.fs.UnixConstants.*;

/*
 * AIX Poller Implementation
 */
public class AhafsPoller extends AbstractPoller
{
    private static final Unsafe UNSAFE = Unsafe.getUnsafe();
    // The mount point for ahafs. Can be mounted at any point, but setup instructions recommend /aha.
    private static final String AHA_MOUNT_POINT = "/aha";
    // The following timeout controls the maximum time a worker thread will remain blocked before
    // picking up newly registered keys.
    private static final int POLL_TIMEOUT = 1_000; // ms
    // This affects the OPAQUE_BUFFER_SIZE.
    private static final int MAX_FDS = 2048;
    // See description of opaque buffer below.
    private static final int OPAQUE_BUFFER_SIZE = MAX_FDS*nPollfdSize();
    // Careful when changing the following buffer size. Keep in sync with the one in AixWatchService.c
    private static final int EVENT_BUFFER_SIZE = 2096;
    private static final int SP_LISTEN = 0;
    private static final int SP_NOTIFY = 1;

    private AixWatchService watcher;
    private HashMap<Integer, AixWatchKey> wdToKey;
    // A socket pair created by the native socket pair routine.
    private int[] sp_signal;
    // Native-memory buffer used by AIX Event Infrastructure (AHAFS)
    // to store file descriptors and other info.
    // Treated as an opaque memory store. Keeping it here allows the
    // native procedures to be functional (as in the paradigm)
    private NativeBuffer opaqueStore;
    // Number of open fds by the system. Managed by the native methods,
    // but stored here for the same reasons as above.
    private int[] nfds;

    public AhafsPoller (AixWatchService watchService)
        throws AixWatchService.FatalWSException
    {
        this.watcher = watchService;
        this.wdToKey = new HashMap<Integer, AixWatchKey>();
        this.nfds = new int[1];
        this.opaqueStore = new NativeBuffer(OPAQUE_BUFFER_SIZE);

        this. sp_signal = new int[2];
        try {
            nSocketpair(sp_signal);
        } catch (UnixException e) {
            throw new AixWatchService.FatalWSException("Could not create socketpair for Poller", e);
        }

        nInit(opaqueStore.address(), OPAQUE_BUFFER_SIZE, nfds, sp_signal[SP_LISTEN]);
    }

    // Wake up poller to process new changes.
    @Override
    void wakeup()
        throws IOException
    {
        // write to socketpair to wakeup polling thread
        try {
            try (NativeBuffer buffer = new NativeBuffer(1)) {
                write(sp_signal[SP_NOTIFY], buffer.address(), 1);
            }
        } catch (UnixException x) {
            throw new IOException("Exception ocurred during poller wakeup " + x.errorString());
        }
    }

    private Path buildAhafsDirMonitorPath(Path path) { return buildAhafsMonitorPath(path, "modDir.monFactory"); }

    private Path buildAhafsFileMonitorPath(Path path) { return buildAhafsMonitorPath(path, "modFile.monFactory"); }

    private Path buildAhafsMonitorPath(Path path, String eventProducer)
    {
        // Create path AHA_MOUNT_POINT/fs/<event-producer>/<parent-dir-path>/<fname>.mon
        return Path.of(AHA_MOUNT_POINT, "fs", eventProducer,
                       path.getParent().toString(), path.getFileName().toString() + ".mon");
    }

    private int createNewWatchDescriptor(UnixPath ahafsMonitorPath)
        throws AixWatchService.FatalWSException
    {
        int wd = AixWatchKey.INVALID_WATCH_DESCRIPTOR;

        // Create resources in AHAFS
        try {
            Files.createDirectories(ahafsMonitorPath.getParent());
        } catch (FileAlreadyExistsException e) {
            // Ignore. It's OK if the parent directory is present in AHAFS.
        } catch (IOException e) {
            throw new AixWatchService.FatalWSException("Unable to create parent directory in AHAFS for " + ahafsMonitorPath, e);
        }

        try {
            try (NativeBuffer strBuff =
                 NativeBuffers.asNativeBuffer(ahafsMonitorPath.getByteArrayForSysCalls())) {
                wd = nRegisterMonitorPath(opaqueStore.address(), nfds[0], strBuff.address());
            }
        } catch (UnixException e) {
            throw new AixWatchService.FatalWSException("Invalid WatchDescriptor returned by native procedure while attempting to register " + ahafsMonitorPath);
        }

        nfds[0] += 1;
        return wd;
    }

    private AixWatchKey.SubKey createSubKey(Path filePath, AixWatchKey.TopLevelKey topLevelKey)
        throws AixWatchService.FatalWSException
    {
        UnixPath monitorPath = (UnixPath) buildAhafsFileMonitorPath(filePath.toAbsolutePath());

        int wd = createNewWatchDescriptor(monitorPath);

        AixWatchKey.SubKey k = new AixWatchKey.SubKey(filePath, wd, topLevelKey);
        wdToKey.put(wd, k);
        return k;
    }

    private void watchSubKeyFiles(Path root, AixWatchKey.TopLevelKey topLevelKey)
        throws AixWatchService.FatalWSException, IOException
    {
        HashSet<AixWatchKey.SubKey> subKeys = new HashSet<>();

        for (Path filePath: Files.walk(root, 1)
                                 .filter((Path p) -> Files.isRegularFile(p))
                                 .collect(Collectors.toList())) {
            subKeys.add(createSubKey(filePath, topLevelKey));
        }

        topLevelKey.addSubKeys(subKeys);
    }

    /**
     * Register a path with the Poller.
     *
     * @return [WatchKey | RuntimeException | IOException] the caller is expected to check
     * if the retuned object is an instance of either exception and act accordingly.
     */
    @Override
    Object implRegister(Path watchPath,
                        Set<? extends WatchEvent.Kind<?>> events,
                        WatchEvent.Modifier... modifiers)
    {
        // System.out.println("[implRegister] Register " + watchPath);
        UnixPath ahafsMonitorPath = (UnixPath)buildAhafsDirMonitorPath(watchPath.toAbsolutePath());


        int wd = AixWatchKey.INVALID_WATCH_DESCRIPTOR;
        try {
            wd = createNewWatchDescriptor(ahafsMonitorPath);
        } catch (AixWatchService.FatalWSException e) {
            return new RuntimeException("Invalid watch descriptor returned for " + ahafsMonitorPath + " during registration of " + watchPath, e);
        }

        AixWatchKey.TopLevelKey wk = new AixWatchKey.TopLevelKey(watchPath, wd, events, this.watcher);
        wdToKey.put(wd, wk);

        // Directory modifications are not supported directly in AIX Event
        // Infrastructure. So the modify event is detected by monitoring
        // for changes to the individual files in the directory.
        if (wk.isWatching(StandardWatchEventKinds.ENTRY_MODIFY)) {
            try {
                watchSubKeyFiles(watchPath, wk);
            } catch (IOException e) {
                return new IOException("[AixWatchService] IO error reported during file registration", e);
            } catch (AixWatchService.FatalWSException e) {
                // Caller checks if return type is insanceof IOE or RuntimeException
                // and then throws. To ensure client is informed of the error, we
                // map our internal exception to RuntimeException and return.
                return new RuntimeException("[AixWatchService] IO error reported during file registration", e);
            }
        }

        // System.out.println("[implRegister] Complete " + watchPath);
        return wk;
    }

    // Cancel single key.
    @Override
    void implCancelKey(WatchKey wk)
    {
        AixWatchKey awk = (AixWatchKey) wk;

        // It this is a TopLevelKey, also cancel SubKeys
        if(wk instanceof AixWatchKey.TopLevelKey) {
            AixWatchKey.TopLevelKey topLevelKey = awk.resolve();
            for (AixWatchKey.SubKey key : topLevelKey.subKeys()) {
                cancelKey(key);
            }
        }

        // Cancel _this_ key.
        cancelKey(awk);
    }

    void cancelKey(AixWatchKey awk)
    {
        nCancelWatchDescriptor(opaqueStore.address(), nfds[0], awk.watchDescriptor());
        try {
            // SubKeys monitor files.
            Path monitorPath = buildAhafsFileMonitorPath(awk.watchable());
            Files.deleteIfExists(monitorPath);
        } catch (IOException e) {
            // System.err.println("Warn: Unable to remove monitor path in AixWatchService for key with (actual) path "
            //                    + awk.watchable());
        }
        wdToKey.remove(awk.watchDescriptor());
    }

    // Cancel all keys. Close poller
    @Override
    void implCloseAll()
    {
        wdToKey.values()
            .stream()
            .forEach((AixWatchKey k) -> k.cancel());

        UnixNativeDispatcher.close(sp_signal[SP_LISTEN], e -> null);
        UnixNativeDispatcher.close(sp_signal[SP_NOTIFY], e -> null);

        nfds[0] = 0;
        opaqueStore.close();
    }

    private int parseWd(String wdLine)
    {
        // Expect: BEGIN_WD=<wd>
        return Integer.parseInt(wdLine.substring(9));
    }

    private Optional<WatchEvent.Kind<?>> parseEventKind(String line, AixWatchKey key)
    {
        if (key instanceof AixWatchKey.TopLevelKey) {
            return parseTopLevelEventKind(line);
        } else {
            return parseSubKeyEventKind(line);
        }
    }

    private Optional<WatchEvent.Kind<?>> parseTopLevelEventKind(String line)
    {
        if (line.equals("RC_FROM_EVPROD=1000")) {
            return Optional.of(StandardWatchEventKinds.ENTRY_CREATE);
        } else if (line.equals("RC_FROM_EVPROD=1002")) {
            return Optional.of(StandardWatchEventKinds.ENTRY_DELETE);
        } else if (line.equals("BUF_WRAP")) {
            return Optional.of(StandardWatchEventKinds.OVERFLOW);
        } else {
            return Optional.empty();
        }
    }

    private Optional<WatchEvent.Kind<?>> parseSubKeyEventKind(String line)
    {
        if (line.startsWith("RC_FROM_EVPROD=")) {
            // SubKey event code is umimportant since all codes map to ENTRY_MODIFY.
            return Optional.of(StandardWatchEventKinds.ENTRY_MODIFY);
        } else if (line.equals("BUF_WRAP")) {
            return Optional.of(StandardWatchEventKinds.OVERFLOW);
        } else {
            return Optional.empty();
        }
    }

    private Iterable<String> getLines(long cBuffer)
    {
        ArrayList<String> lines = new ArrayList<String>();

        int start = 0;
        int pos = start;
        boolean atEnd = false;
        while(!atEnd) {
            byte b = UNSAFE.getByte(cBuffer + pos);

            // Detect end of c-string
            atEnd |= (b == 0);

            // Process new line
            if (b == '\n' || b == 0) {
                byte[] bytes = new byte[pos - start];

                for (int c = 0; c < pos - start; c++) {
                    bytes[c] = UNSAFE.getByte(cBuffer + start + c);
                }

                String line = new String(bytes);
                lines.add(line);

                start = pos + 1;
                pos = start;
            }
            // O.W. Advance cursor until line is complete
            else {
                pos++;
                atEnd |= (pos >= EVENT_BUFFER_SIZE);
            }
        }
        return lines;
    }

    private Iterable<AixWatchService.PollEvent> parsePollEvents(long eventBuffer, int expCount)
    {
        ArrayList<AixWatchService.PollEvent> events = new ArrayList<>();

        if (expCount == 0)
            return events;

        Optional<AixWatchKey> mKey = Optional.empty();
        Optional<WatchEvent.Kind<?>> mKind = Optional.empty();
        Optional<String> mFilename = Optional.empty();

        Iterator<String> lines = getLines(eventBuffer).iterator();
        while (lines.hasNext()) {
            String line = lines.next();
            // Start parsing event.
            if (line.startsWith("BEGIN_WD")) {
                int wd = parseWd(line);
                mKey = Optional.ofNullable(wdToKey.get(wd));
            }
            else if (mKey.isPresent() && line.startsWith("RC_FROM_EVPROD")) {
                mKind = parseEventKind(line, mKey.get());
            }
            else if (line.startsWith("BEGIN_EVPROD_INFO") && lines.hasNext()) {
                // Expect exactly:                     // BEGIN_EVPROD_INFO
                mFilename = Optional.of(lines.next()); // <filename>
                if (lines.hasNext()) lines.next();     // END_EVPROD_INFO
            }
            // Finish parsing event.
            else if (line.startsWith("END_WD")) {
                if (mKey.isPresent() && mKind.isPresent()) {
                    events.add(new AixWatchService.PollEvent(mKind.get(), mKey.get(), mFilename));
                } // else { System.err.println("Warning: Unable to add PollEvent. Kind: " + mKind.toString() + " Key: " + mKey.toString()); }
                mKey = Optional.empty();
                mKind = Optional.empty();
                mFilename = Optional.empty();
            }
        }

        if (events.size() != expCount) {
            // System.err.println("Warning: Poll events missing. Expected: " + expCount + " but found " + events.size());
        }

        return events;
    }

    private void processPollEvent(AixWatchService.PollEvent e)
        throws AixWatchService.FatalWSException
    {
        // Process all events via the parent event since this is the only
        // event expected by the client.
        AixWatchKey.TopLevelKey receiver = e.key().resolve();
        // System.out.println("[processPollEvent] wd: " + e.key().watchDescriptor());

        // Notified of create event, reciever is watching modify
        // Add new file to watched events with reciever as topLevelKey
        if (e.kind() == StandardWatchEventKinds.ENTRY_CREATE &&
            receiver.isWatching(StandardWatchEventKinds.ENTRY_MODIFY)) {
            // System.out.println("New file detected with top level modify");
            String fileName = e.fileName().get();
            createSubKey(Path.of(receiver.watchable().toString(), fileName), receiver);
        // Notified of delete event and reciever is watching modify
        // Remove deleted file from TopLevelKey's SubKey list.
        } else if (e.kind() == StandardWatchEventKinds.ENTRY_DELETE &&
                   receiver.isWatching(StandardWatchEventKinds.ENTRY_MODIFY)) {

            // System.out.println("File deleted with top level modify");
            String fileName = e.fileName().get();
            // cancel subKey, but not the receiver
            implCancelKey(e.key());
        }

        // Only notify keys of events they are watching.
        if (receiver.isWatching(e.kind())) {
            // TODO: If a modify event is detected but the parent is watching only for ... create?
            // build a matrix
            receiver.signalEvent(e.kind(), receiver.watchable());
        }
    }

    // Main poller loop
    @Override
    public void run()
    {
        while (!processRequests()) {
            int evcnt  = 0;
            long evbuf = UNSAFE.allocateMemory(EVENT_BUFFER_SIZE);

            try {
                evcnt = nPoll(opaqueStore.address(), nfds[0], POLL_TIMEOUT, evbuf, EVENT_BUFFER_SIZE);
                for (AixWatchService.PollEvent e: parsePollEvents(evbuf, evcnt)) {
                    processPollEvent(e);
                }
            } catch (Exception e) {
                // TODO: Tidy this logic.
                e.printStackTrace();
                // System.err.println("[AixWatchService] Exiting poll loop with exception: " + e);
                try {
                    close();
                    processRequests();
                } catch (IOException ioe) {
                    // System.err.println("[AixWatchService] Exception while closing poll-loop: " + ioe);
                }
            }
        }
    }

    private static native int nPollfdSize();

    private static native void nInit(long buffer, int buff_size, int[] nv, int socketfd);

    private static native void nCloseAll(long buffer, int nfds);

    private static native void nSocketpair(int[] sv) throws UnixException;

    private static native int nRegisterMonitorPath(long buffer, int nxt_fd, long pathv) throws UnixException;

    private static native int nCancelWatchDescriptor(long buffer, int nfds, int wd);

    private static native int nPoll(long buffer, int nfds, int timeout, long evbuf, int evbuf_size) throws UnixException;

    static {
        jdk.internal.loader.BootLoader.loadLibrary("nio");
    }
}
