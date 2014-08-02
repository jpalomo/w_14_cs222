
- Modify the "CODEROOT" variable in makefile.inc to point to the root of your code base

- Copy your own implementation of rbf to folder, "rbf".

- Implement the Index Manager (IX):

   Go to folder "ix" and type in:

    make clean
    make
    ./ixtest1; ./ixtest2

   The program should work.  But it does nothing.  You are supposed to implement the API of the index manager defined in ix.h

- By default you should not change those functions of the IndexManager and IX_ScanIterator class defined in ix/ix.h. If you think some changes are really necessary, please contact us first.
