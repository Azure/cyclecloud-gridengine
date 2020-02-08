#!/bin/bash
ps -ef | grep sge_shepherd | grep -v grep > /dev/null 2>&1
