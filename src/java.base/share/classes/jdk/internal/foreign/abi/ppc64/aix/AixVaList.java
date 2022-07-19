/*
 *  Copyright (c) 2022, Oracle and/or its affiliates. All rights reserved.
 *  DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 *  This code is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License version 2 only, as
 *  published by the Free Software Foundation.  Oracle designates this
 *  particular file as subject to the "Classpath" exception as provided
 *  by Oracle in the LICENSE file that accompanied this code.
 *
 *  This code is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  version 2 for more details (a copy is included in the LICENSE file that
 *  accompanied this code).
 *
 *  You should have received a copy of the GNU General Public License version
 *  2 along with this work; if not, write to the Free Software Foundation,
 *  Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 *   Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 *  or visit www.oracle.com if you need additional information or have any
 *  questions.
 *
 */
package jdk.internal.foreign.abi.ppc64.aix;

import java.lang.foreign.*;

import jdk.internal.foreign.MemorySessionImpl;
import jdk.internal.foreign.Scoped;
import jdk.internal.foreign.Utils;
import jdk.internal.foreign.abi.SharedUtils;
import jdk.internal.misc.Unsafe;

import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.lang.foreign.MemoryLayout.PathElement.groupElement;
import static jdk.internal.foreign.abi.SharedUtils.SimpleVaArg;
import static jdk.internal.foreign.abi.SharedUtils.THROWING_ALLOCATOR;

public non-sealed class AixVaList implements VaList, Scoped {
    private static final Unsafe U = Unsafe.getUnsafe();

    static final GroupLayout LAYOUT = MemoryLayout.structLayout(
        // TODO
        Aix.C_INT.withName("gp_offset"),
        Aix.C_INT.withName("fp_offset"),
        Aix.C_POINTER.withName("overflow_arg_area"),
        Aix.C_POINTER.withName("reg_save_area")
    ).withName("__va_list_tag");

    private static final long STACK_SLOT_SIZE = 8;

    // TODO
    private static final MemoryLayout GP_REG = MemoryLayout.paddingLayout(64).withBitAlignment(64);
    // TODO
    private static final MemoryLayout FP_REG = MemoryLayout.paddingLayout(128).withBitAlignment(128);

    private static final GroupLayout LAYOUT_REG_SAVE_AREA = MemoryLayout.structLayout(
        // TODO
        GP_REG.withName("%r0"),
        GP_REG.withName("%r1"),
        GP_REG.withName("%r2"),
        GP_REG.withName("%r3"),
        GP_REG.withName("%r4"),
        GP_REG.withName("%r5"),
        FP_REG.withName("%r6"),
        FP_REG.withName("%r7"),
        FP_REG.withName("%r8"),
        FP_REG.withName("%r9"),
        FP_REG.withName("%r10"),
        FP_REG.withName("%r11"),
        FP_REG.withName("%r12"),
        FP_REG.withName("%r13")
    );

    private static final long FP_OFFSET = LAYOUT_REG_SAVE_AREA.byteOffset(groupElement("%xmm0"));

    private static final int GP_SLOT_SIZE = (int) GP_REG.byteSize();
    private static final int FP_SLOT_SIZE = (int) FP_REG.byteSize();

    private static final int MAX_GP_OFFSET = (int) FP_OFFSET; // 6 regs used
    private static final int MAX_FP_OFFSET = (int) LAYOUT_REG_SAVE_AREA.byteSize(); // 8 16 byte regs

    private static final VarHandle VH_fp_offset = LAYOUT.varHandle(groupElement("fp_offset"));
    private static final VarHandle VH_gp_offset = LAYOUT.varHandle(groupElement("gp_offset"));
    private static final VarHandle VH_overflow_arg_area = LAYOUT.varHandle(groupElement("overflow_arg_area"));
    private static final VarHandle VH_reg_save_area = LAYOUT.varHandle(groupElement("reg_save_area"));

    private static final VaList EMPTY = new SharedUtils.EmptyVaList(emptyListAddress());

    private final MemorySegment segment;
    private MemorySegment overflowArgArea;
    private final MemorySegment regSaveArea;
    private final long gpLimit;
    private final long fpLimit;

    private AixVaList(MemorySegment segment,
                       MemorySegment overflowArgArea,
                       MemorySegment regSaveArea, long gpLimit, long fpLimit) {
        this.segment = segment;
        this.overflowArgArea = overflowArgArea;
        this.regSaveArea = regSaveArea;
        this.gpLimit = gpLimit;
        this.fpLimit = fpLimit;
    }

    private static AixVaList readFromSegment(MemorySegment segment) {
        MemorySegment regSaveArea = getRegSaveArea(segment);
        MemorySegment overflowArgArea = getArgOverflowArea(segment);
        // TODO
        return new AixVaList(segment, overflowArgArea, regSaveArea, MAX_GP_OFFSET, MAX_FP_OFFSET);
    }

    private static MemoryAddress emptyListAddress() {
        return null;
    }

    public static VaList empty() {
        // TODO
        return EMPTY;
    }

    private int currentGPOffset() {
        // TODO
        return (int) VH_gp_offset.get(segment);
    }

    private void currentGPOffset(int i) {
        // TODO
        VH_gp_offset.set(segment, i);
    }

    private int currentFPOffset() {
        // TODO
        return (int) VH_fp_offset.get(segment);
    }

    private void currentFPOffset(int i) {
        // TODO
        VH_fp_offset.set(segment, i);
    }

    private static MemorySegment getRegSaveArea(MemorySegment segment) {
        // TODO
        return MemorySegment.ofAddress(((MemoryAddress)VH_reg_save_area.get(segment)),
                LAYOUT_REG_SAVE_AREA.byteSize(), segment.session());
    }

    private static MemorySegment getArgOverflowArea(MemorySegment segment) {
        // TODO
        return MemorySegment.ofAddress(((MemoryAddress)VH_overflow_arg_area.get(segment)),
                Long.MAX_VALUE, segment.session()); // size unknown
    }

    private long preAlignOffset(MemoryLayout layout) {
        // TODO
        return 0L;
    }

    private void setOverflowArgArea(MemorySegment newSegment) {
        // TODO
    }

    private void preAlignStack(MemoryLayout layout) {
        // TODO
        setOverflowArgArea(overflowArgArea.asSlice(preAlignOffset(layout)));
    }

    private void postAlignStack(MemoryLayout layout) {
        // TODO
        setOverflowArgArea(overflowArgArea.asSlice(Utils.alignUp(layout.byteSize(), STACK_SLOT_SIZE)));
    }

    @Override
    public int nextVarg(ValueLayout.OfInt layout) {
        return (int) read(layout);
    }

    @Override
    public long nextVarg(ValueLayout.OfLong layout) {
        return (long) read(layout);
    }

    @Override
    public double nextVarg(ValueLayout.OfDouble layout) {
        return (double) read(layout);
    }

    @Override
    public MemoryAddress nextVarg(ValueLayout.OfAddress layout) {
        return (MemoryAddress) read(layout);
    }

    @Override
    public MemorySegment nextVarg(GroupLayout layout, SegmentAllocator allocator) {
        Objects.requireNonNull(allocator);
        return (MemorySegment) read(layout, allocator);
    }

    private Object read(MemoryLayout layout) {
        return read(layout, THROWING_ALLOCATOR);
    }

    private Object read(MemoryLayout layout, SegmentAllocator allocator) {
        Objects.requireNonNull(layout);
        // TODO
        return null;
    }

    @Override
    public void skip(MemoryLayout... layouts) {
        Objects.requireNonNull(layouts);
        // TODO
    }

    static AixVaList.Builder builder(MemorySession session) {
        // TODO
        return new AixVaList.Builder(session);
    }

    public static VaList ofAddress(MemoryAddress ma, MemorySession session) {
        // TODO
        return readFromSegment(MemorySegment.ofAddress(ma, LAYOUT.byteSize(), session));
    }

    @Override
    public MemorySession session() {
        // TODO
        return segment.session();
    }

    @Override
    public VaList copy() {
        MemorySegment copy = MemorySegment.allocateNative(LAYOUT, segment.session());
        copy.copyFrom(segment);
        // TODO: Check (from SysV)
        return new AixVaList(copy, overflowArgArea, regSaveArea, gpLimit, fpLimit);
    }

    @Override
    public MemoryAddress address() {
        // TODO
        return segment.address();
    }

    private static boolean isRegOverflow(long currentGPOffset, long currentFPOffset, TypeClass typeClass) {
        // TODO
        return false;
    }

    @Override
    public String toString() {
        // TODO
        return "AixVaList{"
               + "gp_offset=" + currentGPOffset()
               + ", fp_offset=" + currentFPOffset()
               + ", overflow_arg_area=" + overflowArgArea
               + ", reg_save_area=" + regSaveArea
               + '}';
    }

    public static non-sealed class Builder implements VaList.Builder {
        private final MemorySession session;
        private final MemorySegment reg_save_area;
        private long currentGPOffset = 0;
        private long currentFPOffset = FP_OFFSET;
        private final List<SimpleVaArg> stackArgs = new ArrayList<>();

        public Builder(MemorySession session) {
            // TODO
            this.session = session;
            this.reg_save_area = MemorySegment.allocateNative(LAYOUT_REG_SAVE_AREA, session);
        }

        @Override
        public Builder addVarg(ValueLayout.OfInt layout, int value) {
            return arg(layout, value);
        }

        @Override
        public Builder addVarg(ValueLayout.OfLong layout, long value) {
            return arg(layout, value);
        }

        @Override
        public Builder addVarg(ValueLayout.OfDouble layout, double value) {
            return arg(layout, value);
        }

        @Override
        public Builder addVarg(ValueLayout.OfAddress layout, Addressable value) {
            return arg(layout, value.address());
        }

        @Override
        public Builder addVarg(GroupLayout layout, MemorySegment value) {
            return arg(layout, value);
        }

        private Builder arg(MemoryLayout layout, Object value) {
            // TODO
            return this;
        }

        private boolean isEmpty() {
            // TODO
            return false;
        }

        public VaList build() {
            if (isEmpty()) {
                return EMPTY;
            }

            SegmentAllocator allocator = SegmentAllocator.newNativeArena(session);
            MemorySegment vaListSegment = allocator.allocate(LAYOUT);
            MemorySegment stackArgsSegment;
            if (!stackArgs.isEmpty()) {
                // TODO
            } else {
                stackArgsSegment = MemorySegment.ofAddress(MemoryAddress.NULL, 0, session);
            }

            // TODO
            return (VaList)(new Object());
            // return new AixVaList(vaListSegment, stackArgsSegment, reg_save_area, currentGPOffset, currentFPOffset);
        }
    }
}
