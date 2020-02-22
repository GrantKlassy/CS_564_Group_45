DROP TABLE IF EXISTS Items;
DROP TABLE IF EXISTS Users;
DROP TABLE IF EXISTS Categories;
DROP TABLE IF EXISTS Bids;

CREATE TABLE Users (
    userID CHAR(100) NOT NULL,
    rating INTEGER NOT NULL,
    location CHAR(100),
    country CHAR(100),
    PRIMARY KEY (userID)
);

CREATE TABLE Items (
    itemID INTEGER NOT NULL,
    name CHAR(100) NOT NULL,
    currently CHAR(100) NOT NULL,
    buyPrice CHAR(100),
    firstBid CHAR(100) NOT NULL,
    numberOfBids INTEGER NOT NULL,
    started CHAR(100) NOT NULL,
    ends CHAR(100) NOT NULL,
    description CHAR(100) NOT NULL,
    sellerID CHAR(100) NOT NULL,
    PRIMARY KEY (itemID),
    FOREIGN KEY (sellerID) REFERENCES User(userID)
);

CREATE TABLE Categories (
    category CHAR(100) NOT NULL,
    itemID INTEGER NOT NULL,
    PRIMARY KEY (category, itemID),
    FOREIGN KEY (itemID) REFERENCES Items(itemID)
);

CREATE TABLE Bids (
    itemID INTEGER NOT NULL,
    userID CHAR(100) NOT NULL,
    bidTime CHAR(100) NOT NULL,
    amount CHAR(100) NOT NULL,
    PRIMARY KEY (itemID, userID, bidTime),
    FOREIGN KEY (itemID) REFERENCES Items(itemID), 
    FOREIGN KEY (userID) REFERENCES Users(userID)
);
