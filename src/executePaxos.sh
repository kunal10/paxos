#!/bin/bash

javac ut/distcomp/framework/*.java ut/distcomp/paxos/*.java
python tester.py
./cleanFolder.sh