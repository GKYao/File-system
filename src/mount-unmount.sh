#!/bin/bash

fusermount -u /tmp/mountdir
make
./sfs /.freespace/testfsfile /tmp/mountdir/
