
include ../makefile.inc

all: libqe.a qetest_1

# lib file dependencies
libqe.a: libqe.a(qe.o)  # and possibly other .o files

# c file dependencies
qe.o: qe.h

qetest.o: qe.h

# binary dependencies
qetest_1: qetest_1.o libqe.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rm/librm.a $(CODEROOT)/rbf/librbf.a

# dependencies to compile used libraries
.PHONY: $(CODEROOT)/rbf/librbf.a
$(CODEROOT)/rbf/librbf.a:
	$(MAKE) -C $(CODEROOT)/rbf librbf.a

.PHONY: $(CODEROOT)/rm/librm.a
$(CODEROOT)/rm/librm.a:
	$(MAKE) -C $(CODEROOT)/rm librm.a

.PHONY: $(CODEROOT)/ix/libix.a
$(CODEROOT)/ix/libix.a:
	$(MAKE) -C $(CODEROOT)/ix libix.a

.PHONY: clean
clean:
	-rm Columns IndexTables left left_B_meta left_B_primary Tables 
	-rm qetest_1 *.a *.o *~
	$(MAKE) -C $(CODEROOT)/rm clean
	$(MAKE) -C $(CODEROOT)/ix clean 
