#!/bin/bash

echo importing csv file
mongoimport --db=Customs --collection=DataSemilla --type=csv --headerline --file=DataSemilla.csv --ignoreBlanks

echo cleaning dates
python ds_clean/date_format.py