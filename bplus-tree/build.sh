#!/bin/bash
CC=gcc
CSTDFLAG="--std=c99 -pedantic -Wall -Wextra -Wno-unused-parameter"
CPPFLAGS="-c -fpic -Iinclude -Iexternal/snappy"
CPPFLAGS+=" -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -D_XOPEN_SOURCE=500 -D_DARWIN_C_SOURCE"
LDFLAGS="-lpthread"
SRC_DIR=src
INCLUDE_DIR=include
OBJ_DIR=obj

for file in "$SRC_DIR"/*.c; do
    base_name=$(basename "$file" .c)
    $CC $CSTDFLAG $CPPFLAGS $LDFLAGS -I"$INCLUDE_DIR" "$file" -o "$OBJ_DIR/$base_name.o"
done

$CC -shared -o libbplustree.so "$OBJ_DIR"/*.o