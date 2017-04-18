#!/bin/bash
rm dependencies.zip
zip dependencies.zip functions.py article.py const.py article_parser.py apps.py pr.py
spark-submit --packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.2 --py-files dependencies.zip main.py
