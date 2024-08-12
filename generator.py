import csv
import random
import string
import datetime
import numpy as np
import re

#paths for datasets and output
BASE_PATH = 'C:/Users/Hunter/Desktop/Revature/Python/Project2'
FNAME_PATH = BASE_PATH + '/datasets/new-top-firstNames.csv'
LNAME_PATH =  BASE_PATH + '/datasets/new-top-surnames.csv'
CITY_PATH = BASE_PATH + '/datasets/worldcities.csv'
PRODUCT_PATH = BASE_PATH + '/datasets/products.csv'
OUT_PATH = BASE_PATH + '/datasets/output.csv'
OUT_LENGTH = 15000
CUSTOMER_AND_PRODUCTS_LENGTH = 1000

#creating list of first names
fnames = []
with open(FNAME_PATH, 'r') as file:
    firstnames = csv.reader(file)
    for entry in firstnames:
        fnames.append(entry[1])

#creating list of last names
lnames = []
with open(LNAME_PATH, 'r') as file:
    lastnames = csv.reader(file)
    for entry in lastnames:
        lnames.append(entry[1])

#generating list of customers with randomly generated ID [NAME, ID]
customers = []
for i in range(CUSTOMER_AND_PRODUCTS_LENGTH):
    #Randomly generated customer ID
    custID = ''.join([random.choice(string.ascii_letters
        + string.digits) for n in range(16)])
    #Randomly selecting a first and last name
    randIndexF = random.randint(0, len(fnames)-1)
    randIndexL = random.randint(0, len(lnames)-1)
    name = fnames[randIndexF].upper() + " " + lnames[randIndexL].upper()

    customers.append([name, custID])

#creating list of cities[CITY, COUNTRY]
cities = []
with open(CITY_PATH, 'r', encoding="utf8") as file:
    cityReader = csv.reader(file)
    for entry in cityReader:
        cities.append([entry[0], entry[4], entry[9]])

#selecting only top 100 most populous cities
cities = sorted(cities, key = lambda x: x[2], reverse=True)
cities = cities[0:100]

#creating product list of products: [NAME, CATEGORY, ID, PRICE]
products = []
with open(PRODUCT_PATH, 'r', encoding="utf8") as file:
    productReader = csv.reader(file)
    i = 0
    for entry in productReader:
        if(i == 0):
            i += 1 
            continue
        price = re.sub('[^0-9.]','', entry[9])
        if price is not '': price = round(float(price)*0.012, 2)
        else: continue
        products.append([entry[1], entry[2], i, price])
        i+=1

#randomly select 1000 from the list of 500,000 products
newProducts = []
for i in range(CUSTOMER_AND_PRODUCTS_LENGTH):
    productsIndex = random.randint(0, len(products)-1)
    newProducts.append(products[productsIndex])
products = newProducts

#Generating list of random products
with open(OUT_PATH, 'w+', encoding="utf8", newline = '') as file:
    out = csv.writer(file)
    headers = ['order_id', 'customer_id', 'customer_name', 
               'product_id', 'product_name', 'product_category', 
               'payment_type', 'qty', 'price', 
               'datetime', 'country', 'city', 
               'ecommerce_website_name', 'payment_txn_id', 'payment_txn_success', 
               'failure reason']
    out.writerow(headers)

    #creating list of random indices generated on a normal distribution
    randomNormalIndices = np.random.normal(CUSTOMER_AND_PRODUCTS_LENGTH/2, CUSTOMER_AND_PRODUCTS_LENGTH/4, OUT_LENGTH)
    randomAdjustedIndices = []
    for i in randomNormalIndices:
        i = int(i)
        if i >= 0 and i < CUSTOMER_AND_PRODUCTS_LENGTH:
            randomAdjustedIndices.append(i)
    
    randomNormalQuantities = np.random.normal(5, 2.5, 1000)
    randomQuantities = []
    for i in randomNormalQuantities:
        i = int(i)
        if (i > 0):
            randomQuantities.append(i)

    for i in range(OUT_LENGTH):
        #Randomly generated order ID
        orderID = ''.join([random.choice(string.ascii_letters
            + string.digits) for n in range(24)])
        
        #Randomly selecting index from list of normally distributed indices and selecting customer with selected index
        randIndex = random.randint(0, len(randomAdjustedIndices)-1)
        randIndexC = randomAdjustedIndices[randIndex]
        customer = customers[randIndexC]

        #Randomly selecting product
        randIndex = random.randint(0, len(randomAdjustedIndices)-1)
        randIndexP = randomAdjustedIndices[randIndex]
        product = products[randIndexP]

        #Randomly selecting payment choice from list
        payment_type = random.choice(['Card', 'Internet Banking', 'UPI', 'Wallet'])

        #Random quantity between 1 and 10
        qtyIndex = random.randint(0, len(randomQuantities)-1)
        qty = randomQuantities[qtyIndex]
        
        #Random date between start (2012) and end (now)
        start = datetime.date(2022, 1, 1)
        end = datetime.datetime.now().date()
        diff = end - start
        diff = diff.days
        #Selecting random date between 2012 and now
        numdays = random.randint(0, diff)
        date = start + datetime.timedelta(int(numdays))

        #randomly selecting city
        randindexC = random.randint(0, len(cities)-1)
        city = cities[randindexC]

        #Randomly selecting website from list
        website = random.choice(['www.amazon.com', 'www.ebay.com', 'www.walmart.com', 
                                 'www.wayfair.com', 'www.apple.com', 'www.homedepot.com', 
                                 'www.costco.com', 'www.alibaba.com', 'www.bestbuy.com', 
                                 'www.nike.com', 'www.etsy.com'])

        paymentID = ''.join([random.choice(string.ascii_letters
            + string.digits) for n in range(16)])
        
        #Generating transaction success - 1 in 20 transactions will decline
        roll = random.randint(1, 20)
        if(roll == 1 or product[3] == ''): 
            success = 'N'
            #selecting random failure reason from list
            reason = random.choice(['Card Declined', 'Fraud Alert', 'Insufficient Funds', 'Expired Card', 'Invalid Billing Information'])
        else:
            success = 'Y'
            reason = ""
        #generating row from randomly generated values
        row = [orderID, customer[1], customer[0], 
               product[2], product[0], product[1], 
               payment_type, qty, product[3], date,
               city[1], city[0], website, paymentID,
                success, reason]
        
        #corrupting ~5 percent of entries with either 'CORRUPTED' or empty field
        corruption = random.randint(1, 100)
        if (corruption <= 5):
            corruptedField = random.randint(0, 15)
            corruptionType = random.choice(['CORRUPTED', None])
            row[corruptedField] = corruptionType

        out.writerow(row)
