#!/bin/bash

CXX=gcc
CFLAGS="-c -fpic"

$CXX $CFLAGS btree2v.c -o btree2v.o

$CXX -shared -o btree.so btree2v.o

