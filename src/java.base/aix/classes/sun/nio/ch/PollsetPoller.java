/*
 * Copyright (c) 2022, Oracle and/or its affiliates. All rights reserved.
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
package sun.nio.ch;

import java.io.IOException;
import sun.nio.ch.AixPollPort;

/**
 * Poller implementation based on the AIX Pollset library.
 */

class PollsetPoller extends Poller {
    private int setid;
    private int setsize;

    PollsetPoller(boolean read) throws IOException {
        super(read);
        this.setsize = 0;
        this.setid = AixPollPort.pollsetCreate();
    }

    @Override
    int fdVal() {
        return setid;
    }

    @Override
    void implRegister(int fd) throws IOException {
        setsize++;

        int ret = AixPollPort.pollsetCtr(setid, PS_MOD, fd, this.read ? Net.POLLIN : Net.POLLOUT);
        if (ret != 0) {
            throw new IOException("Unable to register fd " + fd +
                                    ". Command failed with ERRNO " + ret);
        }
    }

    @Override
    void implDeregister(int fd) {
        size--;

        int ret = AixPollPort.pollsetCtr(setid, PS_DELETE, fd, 0);

        assert ret == 0;
    }

    @Override
    int poll(int _timeout) throws IOException {
        // TODO: use timeout (pollset_poll_ext call takes timeout value)
        long buffer = AixPollPort.allocatePollArray(setsize);

        int n = AixPollPort.pollsetPoll(setid, psarr, setsize);
        for(int i=0; i<n; i++) {
            long eventAddress = AixPollPort.getEvent(address, i);
            int fd = AixPollPort.getDescriptor(eventAddress);
            polled(fd);
        }

        AixPollPort.freePollArray(buffer);
        return n;
    }
}

