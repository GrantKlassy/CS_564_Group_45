#!/bin/bash

# Remove any old .dat files
rm *.dat

# Uncomment to test the small file
#python2 parser.py ebay_data/items-0.json

# Run the parser with all files
python2 parser.py ebay_data/items-*.json

# sort and uniq the *.dat files to remove duplicate entries
for FILE in *.dat; do
	cat $FILE | sort | uniq > $FILE.uniq
	rm $FILE
	mv $FILE.uniq $FILE
done
