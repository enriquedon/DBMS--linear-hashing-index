include ../makefile.inc

all: librm.a rmtest_create_tables rmtest_00 rmtest_01 rmtest_02 rmtest_03 rmtest_04 rmtest_05 rmtest_06 rmtest_07 rmtest_08a rmtest_08b rmtest_09 rmtest_10 rmtest_11 rmtest_12 rmtest_13 rmtest_14 rmtest_15 rmtest_16 rmtest_extra_1 rmtest_extra_2 rmtest_extra_3

# lib file dependencies
librm.a: librm.a(rm.o)  # and possibly other .o files

# c file dependencies
rm.o: rm.h

rmtest_00.o: rm.h test_util.h

rmtest_01.o: rm.h test_util.h

rmtest_02.o: rm.h test_util.h

rmtest_03.o: rm.h test_util.h

rmtest_04.o: rm.h test_util.h

rmtest_05.o: rm.h test_util.h

rmtest_06.o: rm.h test_util.h

rmtest_07.o: rm.h test_util.h

rmtest_08a.o: rm.h test_util.h

rmtest_08b.o: rm.h test_util.h

rmtest_09.o: rm.h test_util.h

rmtest_10.o: rm.h test_util.h

rmtest_11.o: rm.h test_util.h

rmtest_12.o: rm.h test_util.h

rmtest_13.o: rm.h test_util.h

rmtest_14.o: rm.h test_util.h

rmtest_15.o: rm.h test_util.h

rmtest_16.o: rm.h test_util.h

rmtest_extra_1.o: rm.h

rmtest_extra_2.o: rm.h

rmtest_extra_3.o: rm.h

# binary dependencies
rmtest_create_tables: rmtest_create_tables.o librm.a $(CODEROOT)/rbf/librbf.a $(CODEROOT)/ix/libix.a

rmtest_00: rmtest_00.o librm.a $(CODEROOT)/rbf/librbf.a $(CODEROOT)/ix/libix.a 

rmtest_01: rmtest_01.o librm.a $(CODEROOT)/rbf/librbf.a $(CODEROOT)/ix/libix.a 

rmtest_02: rmtest_02.o librm.a $(CODEROOT)/rbf/librbf.a $(CODEROOT)/ix/libix.a 

rmtest_03: rmtest_03.o librm.a $(CODEROOT)/rbf/librbf.a $(CODEROOT)/ix/libix.a 

rmtest_04: rmtest_04.o librm.a $(CODEROOT)/rbf/librbf.a $(CODEROOT)/ix/libix.a 

rmtest_05: rmtest_05.o librm.a $(CODEROOT)/rbf/librbf.a $(CODEROOT)/ix/libix.a 

rmtest_06: rmtest_06.o librm.a $(CODEROOT)/rbf/librbf.a $(CODEROOT)/ix/libix.a 

rmtest_07: rmtest_07.o librm.a $(CODEROOT)/rbf/librbf.a $(CODEROOT)/ix/libix.a 

rmtest_08a: rmtest_08a.o librm.a $(CODEROOT)/rbf/librbf.a $(CODEROOT)/ix/libix.a 

rmtest_08b: rmtest_08b.o librm.a $(CODEROOT)/rbf/librbf.a $(CODEROOT)/ix/libix.a 

rmtest_09: rmtest_09.o librm.a $(CODEROOT)/rbf/librbf.a $(CODEROOT)/ix/libix.a 

rmtest_10: rmtest_10.o librm.a $(CODEROOT)/rbf/librbf.a $(CODEROOT)/ix/libix.a 

rmtest_11: rmtest_11.o librm.a $(CODEROOT)/rbf/librbf.a $(CODEROOT)/ix/libix.a 

rmtest_12: rmtest_12.o librm.a $(CODEROOT)/rbf/librbf.a $(CODEROOT)/ix/libix.a 

rmtest_13: rmtest_13.o librm.a $(CODEROOT)/rbf/librbf.a $(CODEROOT)/ix/libix.a 

rmtest_14: rmtest_14.o librm.a $(CODEROOT)/rbf/librbf.a $(CODEROOT)/ix/libix.a 

rmtest_15: rmtest_15.o librm.a $(CODEROOT)/rbf/librbf.a $(CODEROOT)/ix/libix.a 

rmtest_16: rmtest_16.o librm.a $(CODEROOT)/rbf/librbf.a $(CODEROOT)/ix/libix.a 

rmtest_extra_1: rmtest_extra_1.o librm.a $(CODEROOT)/rbf/librbf.a $(CODEROOT)/ix/libix.a 

rmtest_extra_2: rmtest_extra_2.o librm.a $(CODEROOT)/rbf/librbf.a $(CODEROOT)/ix/libix.a 

rmtest_extra_3: rmtest_extra_3.o librm.a $(CODEROOT)/rbf/librbf.a $(CODEROOT)/ix/libix.a

# dependencies to compile used libraries
.PHONY: $(CODEROOT)/rbf/librbf.a
$(CODEROOT)/rbf/librbf.a:
	$(MAKE) -C $(CODEROOT)/rbf librbf.a


.PHONY: clean
clean:
	-rm rmtest_create_tables rmtest_00 rmtest_01 rmtest_02 rmtest_03 rmtest_04 rmtest_05 rmtest_06 rmtest_07 rmtest_08a rmtest_08b rmtest_09 rmtest_10 rmtest_11 rmtest_12 rmtest_13 rmtest_14 rmtest_15 rmtest_16 rmtest_extra_1 rmtest_extra_2 rmtest_extra_3 *.a *.o *~ 
	$(MAKE) -C $(CODEROOT)/rbf clean