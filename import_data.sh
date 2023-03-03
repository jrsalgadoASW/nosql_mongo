#!/bin/bash

mongoimport --db=Customs --collection=DataSemilla --type=csv --headerline --file=DataSemilla.csv
