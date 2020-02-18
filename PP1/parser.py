#!/usr/bin/python2
"""
FILE: parser.py
------------------
Author: Firas Abuzaid (fabuzaid@stanford.edu)
Author: Perth Charernwattanagul (puch@stanford.edu)
Modified: 04/21/2014

Skeleton parser for CS564 programming project 1. Has useful imports and
functions for parsing, including:

1) Directory handling -- the parser takes a list of eBay json files
and opens each file inside of a loop. You just need to fill in the rest.
2) Dollar value conversions -- the json files store dollar value amounts in
a string like $3,453.23 -- we provide a function to convert it to a string
like XXXXX.xx.
3) Date/time conversions -- the json files store dates/ times in the form
Mon-DD-YY HH:MM:SS -- we wrote a function (transformDttm) that converts to the
for YYYY-MM-DD HH:MM:SS, which will sort chronologically in SQL.

Your job is to implement the parseJson function, which is invoked on each file by
the main function. We create the initial Python dictionary object of items for
you; the rest is up to you!
Happy parsing!
"""

import sys
from json import loads
from re import sub

columnSeparator = "|"

# Dictionary of months used for date transformation
MONTHS = {'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06',\
        'Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}

"""
Returns true if a file ends in .json
"""
def isJson(f):
    return len(f) > 5 and f[-5:] == '.json'

"""
Converts month to a number, e.g. 'Dec' to '12'
"""
def transformMonth(mon):
    if mon in MONTHS:
        return MONTHS[mon]
    else:
        return mon

"""
Transforms a timestamp from Mon-DD-YY HH:MM:SS to YYYY-MM-DD HH:MM:SS
"""
def transformDttm(dttm):
    dttm = dttm.strip().split(' ')
    dt = dttm[0].split('-')
    date = '20' + dt[2] + '-'
    date += transformMonth(dt[0]) + '-' + dt[1]
    return date + ' ' + dttm[1]

"""
Transform a dollar value amount from a string like $3,453.23 to XXXXX.xx
"""

def transformDollar(money):
    if money == None or len(money) == 0:
        return money
    return sub(r'[^\d.]', '', money)

"""
Parses a single json file. Currently, there's a loop that iterates over each
item in the data set. Your job is to extend this functionality to create all
of the necessary SQL tables for your database.
"""
def parseJson(json_file):
    with open(json_file, 'r') as f:
        items = loads(f.read())['Items'] # creates a Python dictionary of Items for the supplied json file

        # Data structure to hold filenames and filehandles
        files = {
                    'Categories'     : {
                                        'outfile_name'       : 'Categories.dat',
                                        'outfile_handle'     : ''
                                       },
                    'Users'          : {
                                        'outfile_name'       : 'Users.dat',
                                        'outfile_handle'     : ''
                                       },
                    'Items'          : {
                                        'outfile_name'       : 'Items.dat',
                                        'outfile_handle'     : ''
                                       },
                    'Bids'           : {
                                        'outfile_name'       : 'Bids.dat',
                                        'outfile_handle'     : ''
                                       }
                }

        # Open all of the .dat files that we'll be writing to
        for relation in files.keys():
            files[relation]['outfile_handle'] = open(files[relation]['outfile_name'], 'a')

        # For every item...
        for item in items:

            # Get all of the item fields
            ItemID = item.get('ItemID')
            Name = item.get('Name')
            Currently = item.get('Currently')
            Buy_Price = item.get('Buy_Price', "NULL")
            First_Bid = item.get('First_Bid')
            Number_of_Bids = item.get('Number_of_Bids')
            Started = item.get('Started')
            Ends = item.get('Ends')
            Description = item.get('Description')
            Seller_UserID = item.get('Seller').get('UserID')

            # Create and write the string for the item
            itemStr = str(ItemID) + columnSeparator
            itemStr += str(Name) + columnSeparator
            itemStr += str(Currently) + columnSeparator
            itemStr += str(Buy_Price) + columnSeparator
            itemStr += str(First_Bid) + columnSeparator
            itemStr += str(Number_of_Bids) + columnSeparator
            itemStr += str(Started) + columnSeparator
            itemStr += str(Ends) + columnSeparator
            itemStr += str(Description) + columnSeparator
            itemStr += str(Seller_UserID) + "\n"
            files['Items']['outfile_handle'].write(itemStr)

            # Get the item fields that relate to the seller
            Seller_Rating = item.get('Seller').get('Rating')
            Seller_Location = item.get('Location')
            Seller_Country = item.get('Country')

            # Create and write the string for the seller user
            sellerUserStr = str(Seller_UserID) + columnSeparator
            sellerUserStr += str(Seller_Rating) + columnSeparator
            sellerUserStr += str(Seller_Location) + columnSeparator
            sellerUserStr += str(Seller_Country) + "\n"
            files['Users']['outfile_handle'].write(sellerUserStr)

            # Categories
            Categories = []
            Raw_Categories = item.get('Category')
            for Category in Raw_Categories:
                # Create and write the category string
                categoryStr = str(Category) + columnSeparator + str(ItemID) + "\n"
                files['Categories']['outfile_handle'].write(categoryStr)

            # Bids
            Raw_Bids = item.get('Bids')
            if Raw_Bids is not None:

                # For each bid index...
                for index in range(len(Raw_Bids)):

                    # Get all of the bidder fields
                    Bidder_UserID = Raw_Bids[index].get('Bid').get('Bidder').get('UserID')
                    Bidder_Rating = Raw_Bids[index].get('Bid').get('Bidder').get('Rating')
                    Bidder_Country = Raw_Bids[index].get('Bid').get('Bidder').get('Country', "NULL")
                    Bidder_Location = Raw_Bids[index].get('Bid').get('Bidder').get('Location', "NULL")

                    # Create and write the string for the bidder user
                    bidderUserStr = str(Bidder_UserID) + columnSeparator
                    bidderUserStr += str(Bidder_Rating) + columnSeparator
                    bidderUserStr += str(Bidder_Location) + columnSeparator
                    bidderUserStr += str(Bidder_Country) + "\n"
                    files['Users']['outfile_handle'].write(bidderUserStr)

                    # Transform the things that need to be transformed
                    Raw_Time = Raw_Bids[index].get('Bid').get('Time')
                    Raw_Amount = Raw_Bids[index].get('Bid').get('Amount')
                    Time = transformDttm(Raw_Time)
                    Amount = transformDollar(Raw_Amount)

                    # Create and write the bid string
                    bidStr = str(ItemID) + columnSeparator
                    bidStr += str(Bidder_UserID) + columnSeparator
                    bidStr += str(Time) + columnSeparator
                    bidStr += str(Amount) + "\n"
                    files['Bids']['outfile_handle'].write(bidStr)




    pass

    """
    Loops through each json files provided on the command line and passes each file
    to the parser
    """
def main(argv):
    if len(argv) < 2:
        print >> sys.stderr, 'Usage: python skeleton_json_parser.py <path to json files>'
        sys.exit(1)
    # loops over all .json files in the argument
    for f in argv[1:]:
        if isJson(f):
            parseJson(f)
            print "Success parsing " + f

if __name__ == '__main__':
    main(sys.argv)
