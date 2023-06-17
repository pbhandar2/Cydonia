#! /usr/bin/env bash

cd ${HOME}
if [ ! -d "${HOME}/disk/CacheLib" ]; then
    git clone https://github.com/pbhandar2/CacheLib
fi
cd CacheLib 
git checkout active
./contrib/build.sh -j -d 