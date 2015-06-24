#!/bin/sh

version=`cat VERSION`
name=walb
tarprefix=${name}_${version}/
tarfile=${name}_${version}.tar

git archive HEAD --format=tar --prefix $tarprefix -o $tarfile
tar f $tarfile --wildcards --delete '*/.gitignore'
gzip $tarfile
