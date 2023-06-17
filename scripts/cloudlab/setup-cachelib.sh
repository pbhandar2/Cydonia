#! /usr/bin/env bash

cd ${HOME}
if [ ! -d "${HOME}/disk/CacheLib" ]; then
    git clone https://github.com/pbhandar2/CacheLib
fi
cd ${HOME}/disk/CacheLib
if [ ! -d "${HOME}/disk/CacheLib/phdthesis" ]; then
    git clone https://github.com/pbhandar2/phdthesis
fi
git checkout active
./contrib/build.sh -j -d 