#!/bin/bash

scp -i cs5630s17.pem cs5630s17.pem "ec2-user@$1:~/.ssh/cs5630s17.pem"
scp -i cs5630s17.pem functions.py "ec2-user@$1:~"
scp -i cs5630s17.pem test.py "ec2-user@$1:~"
scp -i cs5630s17.pem check.py "ec2-user@$1:~"
scp -i cs5630s17.pem launch-yarn.bash "ec2-user@$1:~"
