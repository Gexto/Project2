from pyspark import SparkContext, SparkConf

# Removing commas from countries with commas
def clean_country_with_commas(row):
    row_str = str(row).replace("\"", "")

    if row.count("Bonaire, Sint Eustatius, and Saba") > 0:
        index = row_str.index("Bonaire, Sint Eustatius, and Saba")
        return row_str[:index] + "Bonaire Sint Eustatius and Saba," + row_str[index + 34:]

    if row_str.count("Korea, South") > 0:
        index = row_str.index("Korea, South")
        return row_str[:index] + "South Korea," + row_str[index + 13:]
    
    if row_str.count("Korea, North") > 0:
        index = row_str.index("Korea, North")
        return row_str[:index] + "North Korea," + row_str[index + 13:]
    
    return row_str

# Combine product sub-categories into one column     
def combine_category_indexes(row):
    if len(row) > 16:
        combined_col = row[5] + row[6]
        combined_col.replace("\"", "")
        row.pop(6)
        row[5] = combined_col
    
    return row

# Omit any record with missing/false data
def filter_rogue_rows(row):
    if row[9] == '00000000000':
        return False
    
    for i in range(15):
        if row[i] == "":
            return False
    return True

# Flatten list of lists and add commas to create .csv
def add_commas(row):
    combined_row = ''

    for i in range(len(row)):
        combined_row = combined_row + row[i] + ','

    return combined_row[0:-1]

def split_dates(row):
    combined_row = row[9].split(' ')
    row[9] = combined_row[0]
    time = combined_row[1].split(':')
    row.append(time[0])
    row.append(time[1])
    row.append(time[2])


    return row


conf = SparkConf().setAppName("Example1").setMaster("local")
sc = SparkContext(conf=conf)

raw_data = sc.textFile("hdfs:///jerry/data_team_4.csv")

cleaned_countries = raw_data.map(lambda x: clean_country_with_commas(x))

split_data = cleaned_countries.map(lambda x: x.split(","))

combined_indexes = split_data.map(lambda x: combine_category_indexes(x))

rogue_row_free = combined_indexes.filter(lambda x: filter_rogue_rows(x))

split_date = rogue_row_free.map(lambda x: split_dates(x))

flattened_rdd = split_date.map(lambda x: add_commas(x))

flattened_rdd.saveAsTextFile('hdfs:///jerry/proj2output')
