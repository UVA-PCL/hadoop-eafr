#!/bin/bash
git diff hadoop-2.8.1-original -- . ':(exclude)**/pom.xml' ':(exclude)pom.xml' ':(exclude)eafr.patch' ':(exclude)make-eafr-patch.sh' >eafr.patch
