# Relational Database Management System
A relational database implementation written in C++ featuring a file manager, relation manager, index manager, and query engine, as part of UCI's CS 222P: Principles of Data Management course.
## Developed By
Harry Pham and Keith Tachibana
## Development
#### Getting Started
- _*_ Modify the "CODEROOT" variable in makefile.inc to point to the root of your codebase. Usually, this is not necessary.

- _*_ Copy your own implementation of rbf, ix, and rm to folder, "rbf", "ix", and "rm", respectively.
  Don't forget to include RM extension parts in the rm.h file after you copy your code into "rm" folder.
  
- _*_ Implement the extension of Relation Manager (RM) to coordinate data files and the associated indices of the data files.

- _*_ Also, implement Query Engine (QE)

   Go to folder "qe" and type in:
    ```shell
    make clean
    make
    ./qetest_01
    ```
   The program should work. But it does nothing until you implement the extension of RM and QE.

- _*_ If you want to try CLI:

   Go to folder "cli" and type in:
   ```shell
   make clean
   make
   ./cli_example_01
   ```
   or
   ```shell
   ./start
   ```
- _*_ The program should work. But you need to implement the extension of RM and QE to run this program properly. Note that examples in the cli directory are provided for your convenience. These examples are not the public test cases.

- _*_ By default you should not change those classes defined in rm/rm.h and qe/qe.h. If you think some changes are really necessary, please contact us first.
