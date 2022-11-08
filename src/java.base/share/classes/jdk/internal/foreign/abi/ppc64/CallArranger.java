/*
 * Copyright (c) 2022, Oracle and/or its affiliates. All rights reserved.
 * Copyright (c) 2022 SAP SE. All rights reserved.
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
package jdk.internal.foreign.abi.ppc64;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.GroupLayout;
import java.lang.foreign.MemoryAddress;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import jdk.internal.foreign.abi.ABIDescriptor;
import jdk.internal.foreign.abi.Binding;
import jdk.internal.foreign.abi.CallingSequence;
import jdk.internal.foreign.abi.CallingSequenceBuilder;
import jdk.internal.foreign.abi.DowncallLinker;
import jdk.internal.foreign.abi.UpcallLinker;
import jdk.internal.foreign.abi.SharedUtils;
import jdk.internal.foreign.abi.VMStorage;
import jdk.internal.foreign.abi.ppc64.linux.LinuxPPC64CallArranger;
import jdk.internal.foreign.Utils;

import java.lang.foreign.MemorySession;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Optional;

import static jdk.internal.foreign.PlatformLayouts.*;
import static jdk.internal.foreign.abi.ppc64.PPC64Architecture.*;

/**
 * For the PPC64 C ABI specifically, this class uses CallingSequenceBuilder
 * to translate a C FunctionDescriptor into a CallingSequence, which can then be turned into a MethodHandle.
 *
 * This includes taking care of synthetic arguments like pointers to return buffers for 'in-memory' returns.
 *
 * There are minor differences between the ABIs implemented on Linux and AIX
 * which are handled in sub-classes. Clients should access these through the provided
 * public constants CallArranger.LINUX.
 */
public abstract class CallArranger {
    // Linux PPC64 Little Endian uses ABI v2.
    private static final boolean useABIv2 = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;
    private static final int STACK_SLOT_SIZE = 8;
    public static final int MAX_REGISTER_ARGUMENTS = 8;

    // This is derived from the 64-Bit ELF V2 ABI spec, restricted to what's
    // possible when calling to/from C code.
    private static final ABIDescriptor C = abiFor(
        new VMStorage[] { r3, r4, r5, r6, r7, r8, r9, r10 }, // GP input
        new VMStorage[] { f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13 }, // FP intput
        new VMStorage[] { r3 }, // GP output
        new VMStorage[] { f1 }, // FP output
        new VMStorage[] { r0, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12 }, // volatile GP
        new VMStorage[] { f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13 }, // volatile FP
        16, // Stack is always 16 byte aligned on PPC64
        useABIv2 ? 96 : 112, // ABI header
        r12, // target addr reg
        r3   // return buffer addr reg (hidden first argument)
    );

    // record
    public static class Bindings {
        public final CallingSequence callingSequence;
        public final boolean isInMemoryReturn;

        Bindings(CallingSequence callingSequence, boolean isInMemoryReturn) {
            this.callingSequence = callingSequence;
            this.isInMemoryReturn = isInMemoryReturn;
        }
    }

    public static final CallArranger LINUX = new LinuxPPC64CallArranger();

    /**
     * Are variadic arguments assigned to registers as in the standard calling
     * convention, or always passed on the stack?
     *
     * @return true if variadic arguments should be spilled to the stack.
     */
    protected boolean varArgsOnStack() { return false; }

    protected CallArranger() {}

    public Bindings getBindings(MethodType mt, FunctionDescriptor cDesc, boolean forUpcall) {
        CallingSequenceBuilder csb = new CallingSequenceBuilder(C, forUpcall);

        BindingCalculator argCalc = forUpcall ? new BoxBindingCalculator(true) : new UnboxBindingCalculator(true);
        BindingCalculator retCalc = forUpcall ? new UnboxBindingCalculator(false) : new BoxBindingCalculator(false);

        boolean returnInMemory = isInMemoryReturn(cDesc.returnLayout());
        if (returnInMemory) {
            Class<?> carrier = MemoryAddress.class;
            MemoryLayout layout = PPC64.C_POINTER;
            csb.addArgumentBindings(carrier, layout, argCalc.getBindings(carrier, layout));
        } else if (cDesc.returnLayout().isPresent()) {
            Class<?> carrier = mt.returnType();
            MemoryLayout layout = cDesc.returnLayout().get();
            csb.setReturnBindings(carrier, layout, retCalc.getBindings(carrier, layout));
        }

        for (int i = 0; i < mt.parameterCount(); i++) {
            Class<?> carrier = mt.parameterType(i);
            MemoryLayout layout = cDesc.argumentLayouts().get(i);
            if (varArgsOnStack() && SharedUtils.isVarargsIndex(cDesc, i)) {
                argCalc.storageCalculator.adjustForVarArgs();
            }
            csb.addArgumentBindings(carrier, layout, argCalc.getBindings(carrier, layout));
        }

        return new Bindings(csb.build(), returnInMemory);
    }

    public MethodHandle arrangeDowncall(MethodType mt, FunctionDescriptor cDesc) {
        Bindings bindings = getBindings(mt, cDesc, false);

        MethodHandle handle = new DowncallLinker(C, bindings.callingSequence).getBoundMethodHandle();

        if (bindings.isInMemoryReturn) {
            handle = SharedUtils.adaptDowncallForIMR(handle, cDesc);
        }

        return handle;
    }

    public MemorySegment arrangeUpcall(MethodHandle target, MethodType mt, FunctionDescriptor cDesc, MemorySession session) {
        Bindings bindings = getBindings(mt, cDesc, true);

        if (bindings.isInMemoryReturn) {
            target = SharedUtils.adaptUpcallForIMR(target, true /* drop return, since we don't have bindings for it */);
        }

        return UpcallLinker.make(C, target, bindings.callingSequence, session);
    }

    private static boolean isInMemoryReturn(Optional<MemoryLayout> returnLayout) {
        return returnLayout
            .filter(GroupLayout.class::isInstance)
            .isPresent();
    }

    class StorageCalculator {
        private final boolean forArguments;
        private boolean forVarArgs = false;

        private final int[] nRegs = new int[] { 0, 0 };
        private long stackOffset = 0;

        public StorageCalculator(boolean forArguments) {
            this.forArguments = forArguments;
        }

        VMStorage stackAlloc(long size, long alignment) {
            // TODO: Check!
            //assert forArguments : "no stack returns";
            VMStorage storage = stackStorage((int)stackOffset);
            stackOffset++;
            return storage;
        }

        VMStorage stackAlloc(MemoryLayout layout) {
            return stackAlloc(layout.byteSize(), SharedUtils.alignment(layout, true));
        }

        VMStorage[] regAlloc(int type, int count) {
            if (nRegs[0] + nRegs[1] + count <= (forArguments ? MAX_REGISTER_ARGUMENTS : 1)) {
                VMStorage[] source =
                    (forArguments ? C.inputStorage : C.outputStorage)[type];
                VMStorage[] result = new VMStorage[count];
                for (int i = 0; i < count; i++) {
                    result[i] = source[nRegs[type]++];
                }
                return result;
            }
            return null; // use stack
        }

        VMStorage[] regAlloc(int type, MemoryLayout layout) {
            return regAlloc(type, (int)Utils.alignUp(layout.byteSize(), 8) / 8);
        }

        VMStorage nextStorage(int type, MemoryLayout layout) {
            VMStorage[] storage = regAlloc(type, 1);
            // PPC64 requires spill slot for each register argument.
            // So, allocate stack space regardless of reg or stack usage.
            VMStorage stack = stackAlloc(layout);
            return storage == null ? stack : storage[0];
        }

        void adjustForVarArgs() {
            // This system passes all variadic parameters on the stack. Ensure
            // no further arguments are allocated to registers.
            nRegs[StorageClasses.INTEGER] = MAX_REGISTER_ARGUMENTS;
            nRegs[StorageClasses.FLOAT] = MAX_REGISTER_ARGUMENTS;
            forVarArgs = true;
        }
    }

    abstract class BindingCalculator {
        protected final StorageCalculator storageCalculator;

        protected BindingCalculator(boolean forArguments) {
            this.storageCalculator = new StorageCalculator(forArguments);
        }

        protected void spillStructUnbox(Binding.Builder bindings, MemoryLayout layout) {
            // If a struct has been assigned register or HFA class but
            // there are not enough free registers to hold the entire
            // struct, it must be passed on the stack. I.e. not split
            // between registers and stack.

            long offset = 0;
            while (offset < layout.byteSize()) {
                long copy = Math.min(layout.byteSize() - offset, STACK_SLOT_SIZE);
                VMStorage storage =
                    storageCalculator.stackAlloc(copy, STACK_SLOT_SIZE);
                if (offset + STACK_SLOT_SIZE < layout.byteSize()) {
                    bindings.dup();
                }
                Class<?> type = SharedUtils.primitiveCarrierForSize(copy, false);
                bindings.bufferLoad(offset, type)
                        .vmStore(storage, type);
                offset += STACK_SLOT_SIZE;
            }
        }

        protected void spillStructBox(Binding.Builder bindings, MemoryLayout layout) {
            // If a struct has been assigned register or HFA class but
            // there are not enough free registers to hold the entire
            // struct, it must be passed on the stack. I.e. not split
            // between registers and stack.

            long offset = 0;
            while (offset < layout.byteSize()) {
                long copy = Math.min(layout.byteSize() - offset, STACK_SLOT_SIZE);
                VMStorage storage =
                    storageCalculator.stackAlloc(copy, STACK_SLOT_SIZE);
                Class<?> type = SharedUtils.primitiveCarrierForSize(copy, false);
                bindings.dup()
                        .vmLoad(storage, type)
                        .bufferStore(offset, type);
                offset += STACK_SLOT_SIZE;
            }
        }

        abstract List<Binding> getBindings(Class<?> carrier, MemoryLayout layout);
    }

    class UnboxBindingCalculator extends BindingCalculator {
        UnboxBindingCalculator(boolean forArguments) {
            super(forArguments);
        }

        @Override
        List<Binding> getBindings(Class<?> carrier, MemoryLayout layout) {
            TypeClass argumentClass = TypeClass.classifyLayout(layout);
            Binding.Builder bindings = Binding.builder();
            switch (argumentClass) {
                case STRUCT_REGISTER: {
                    assert carrier == MemorySegment.class;
                    VMStorage[] regs = storageCalculator.regAlloc(
                        StorageClasses.INTEGER, layout);
                    if (regs != null) {
                        int regIndex = 0;
                        long offset = 0;
                        while (offset < layout.byteSize()) {
                            final long copy = Math.min(layout.byteSize() - offset, 8);
                            VMStorage storage = regs[regIndex++];
                            boolean useFloat = storage.type() == StorageClasses.FLOAT;
                            Class<?> type = SharedUtils.primitiveCarrierForSize(copy, useFloat);
                            if (offset + copy < layout.byteSize()) {
                                bindings.dup();
                            }
                            bindings.bufferLoad(offset, type)
                                    .vmStore(storage, type);
                            offset += copy;
                        }
                    } else {
                        spillStructUnbox(bindings, layout);
                    }
                    break;
                }
                case STRUCT_REFERENCE: {
                    assert carrier == MemorySegment.class;
                    bindings.copy(layout)
                            .unboxAddress(MemorySegment.class);
                    VMStorage storage = storageCalculator.nextStorage(
                        StorageClasses.INTEGER, PPC64.C_POINTER);
                    bindings.vmStore(storage, long.class);
                    break;
                }
                case STRUCT_HFA: {
                    assert carrier == MemorySegment.class;
                    GroupLayout group = (GroupLayout)layout;
                    VMStorage[] regs = storageCalculator.regAlloc(
                        StorageClasses.FLOAT, group.memberLayouts().size());
                    if (regs != null) {
                        long offset = 0;
                        for (int i = 0; i < group.memberLayouts().size(); i++) {
                            VMStorage storage = regs[i];
                            final long size = group.memberLayouts().get(i).byteSize();
                            boolean useFloat = storage.type() == StorageClasses.FLOAT;
                            Class<?> type = SharedUtils.primitiveCarrierForSize(size, useFloat);
                            if (i + 1 < group.memberLayouts().size()) {
                                bindings.dup();
                            }
                            bindings.bufferLoad(offset, type)
                                    .vmStore(storage, type);
                            offset += size;
                        }
                    } else {
                        spillStructUnbox(bindings, layout);
                    }
                    break;
                }
                case POINTER: {
                    bindings.unboxAddress(carrier);
                    VMStorage storage =
                        storageCalculator.nextStorage(StorageClasses.INTEGER, layout);
                    bindings.vmStore(storage, long.class);
                    break;
                }
                case INTEGER: {
                    VMStorage storage =
                        storageCalculator.nextStorage(StorageClasses.INTEGER, layout);
                    bindings.vmStore(storage, carrier);
                    break;
                }
                case FLOAT: {
                    VMStorage storage =
                        storageCalculator.nextStorage(StorageClasses.FLOAT, layout);
                    bindings.vmStore(storage, carrier);
                    break;
                }
                default:
                    throw new UnsupportedOperationException("Unhandled class " + argumentClass);
            }
            return bindings.build();
        }
    }

    class BoxBindingCalculator extends BindingCalculator {
        BoxBindingCalculator(boolean forArguments) {
            super(forArguments);
        }

        @Override
        List<Binding> getBindings(Class<?> carrier, MemoryLayout layout) {
            TypeClass argumentClass = TypeClass.classifyLayout(layout);
            Binding.Builder bindings = Binding.builder();
            switch (argumentClass) {
                case STRUCT_REGISTER: {
                    assert carrier == MemorySegment.class;
                    bindings.allocate(layout);
                    VMStorage[] regs = storageCalculator.regAlloc(
                        StorageClasses.INTEGER, layout);
                    if (regs != null) {
                        int regIndex = 0;
                        long offset = 0;
                        while (offset < layout.byteSize()) {
                            final long copy = Math.min(layout.byteSize() - offset, 8);
                            VMStorage storage = regs[regIndex++];
                            bindings.dup();
                            boolean useFloat = storage.type() == StorageClasses.FLOAT;
                            Class<?> type = SharedUtils.primitiveCarrierForSize(copy, useFloat);
                            bindings.vmLoad(storage, type)
                                    .bufferStore(offset, type);
                            offset += copy;
                        }
                    } else {
                        spillStructBox(bindings, layout);
                    }
                    break;
                }
                case STRUCT_REFERENCE: {
                    assert carrier == MemorySegment.class;
                    VMStorage storage = storageCalculator.nextStorage(
                        StorageClasses.INTEGER, PPC64.C_POINTER);
                    bindings.vmLoad(storage, long.class)
                            .boxAddress()
                            .toSegment(layout);
                    break;
                }
                case STRUCT_HFA: {
                    assert carrier == MemorySegment.class;
                    bindings.allocate(layout);
                    GroupLayout group = (GroupLayout)layout;
                    VMStorage[] regs = storageCalculator.regAlloc(
                        StorageClasses.FLOAT, group.memberLayouts().size());
                    if (regs != null) {
                        long offset = 0;
                        for (int i = 0; i < group.memberLayouts().size(); i++) {
                            VMStorage storage = regs[i];
                            final long size = group.memberLayouts().get(i).byteSize();
                            boolean useFloat = storage.type() == StorageClasses.FLOAT;
                            Class<?> type = SharedUtils.primitiveCarrierForSize(size, useFloat);
                            bindings.dup()
                                    .vmLoad(storage, type)
                                    .bufferStore(offset, type);
                            offset += size;
                        }
                    } else {
                        spillStructBox(bindings, layout);
                    }
                    break;
                }
                case POINTER: {
                    VMStorage storage =
                        storageCalculator.nextStorage(StorageClasses.INTEGER, layout);
                    bindings.vmLoad(storage, long.class)
                            .boxAddress();
                    break;
                }
                case INTEGER: {
                    VMStorage storage =
                        storageCalculator.nextStorage(StorageClasses.INTEGER, layout);
                    bindings.vmLoad(storage, carrier);
                    break;
                }
                case FLOAT: {
                    VMStorage storage =
                        storageCalculator.nextStorage(StorageClasses.FLOAT, layout);
                    bindings.vmLoad(storage, carrier);
                    break;
                }
                default:
                    throw new UnsupportedOperationException("Unhandled class " + argumentClass);
            }
            return bindings.build();
        }
    }
}
