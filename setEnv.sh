#!/bin/bash
export "GOPATH=$PWD"
export "GO111MODULE=off"
rm -rf .github/workflows/classroom.yml
curl https://raw.githubusercontent.com/HuadongHu/autograding/master/classroom.yml >> .github/workflows/classroom.yml