#!/usr/bin/env bash
cd test
./*
RESULT=$?
[ $RESULT -ne 0 ] && exit 1
exit 0