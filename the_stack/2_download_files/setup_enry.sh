#!/bin/bash

wget -O ~/go.tar.gz https://go.dev/dl/go1.20.6.linux-amd64.tar.gz
tar -C ~ -xzf ~/go.tar.gz
rm ~/go.tar.gz

if ! grep -q "export PATH=\$PATH:~/go/bin" ~/.bashrc ; then
    # If not found, add Go to your PATH
    echo "export PATH=\$PATH:~/go/bin" >> ~/.bashrc
fi
source ~/.bashrc

git clone https://github.com/go-enry/go-enry/
cd go-enry/python || exit 1
source activate pyspark

pushd .. && make static && popd
pip install -r requirements.txt
python build_enry.py
pip install -e .

