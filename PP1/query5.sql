
SELECT COUNT(*) FROM (
	SELECT DISTINCT SellerID
	FROM Items, Users
	WHERE Items.sellerID = Users.userID AND Users.rating > 1000)

