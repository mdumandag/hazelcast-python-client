#!/usr/bin/env bash
python map_soak.py --hour $1 --addresses $2 --log $3-$4 > ./logs/client-out-$4 2> ./logs/client-err-$4 &