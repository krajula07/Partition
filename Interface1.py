import psycopg2
import os
import sys


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    print("kavya")
    c = openconnection.cursor()
    c.execute("drop table if exists "+ratingstablename+";")
    c.execute(" CREATE TABLE "+ratingstablename+"(userid int, movieid int, rating float);")
    f = open(ratingsfilepath,"r")
    while True:        
        readl = f.readline()
        if not readl:
            break        
        data = readl.split("::")       
        c.execute("INSERT INTO "+ratingstablename+" VALUES(" + data[0] + ", " + data[1] + ", " + data[2]+ ");")       
    c.close()
    openconnection.commit()

def rangePartition(ratingstablename, numberofpartitions, openconnection):
    c = openconnection.cursor()
    Range = float(5)/numberofpartitions
    min_Rating = 0
    max_Rating = min_Rating + Range   
    RANGE_TABLE_PREFIX = 'range_ratings_part'   
    for i in range(0,numberofpartitions):
        c.execute("drop table if exists "+(RANGE_TABLE_PREFIX + str(i))+";")
        c.execute("create table " +(RANGE_TABLE_PREFIX + str(i))+ "(userid int, movieid int, rating float);" )                
        if i == 0:           
            Query = "insert into "+(RANGE_TABLE_PREFIX + str(i))+"(userid, movieid, rating) (select userid, movieid, rating from "+ratingstablename+" where rating >= "+str(min_Rating)+" and rating <= "+str(max_Rating)+");" 
        else:            
            Query = "insert into "+(RANGE_TABLE_PREFIX + str(i))+"(userid, movieid, rating) (select userid, movieid, rating from "+ratingstablename+" where rating > "+str(min_Rating)+" and rating <= "+str(max_Rating)+");"
        c.execute(Query)       
        min_Rating = max_Rating
        max_Rating = max_Rating + Range 
    c.close()
    openconnection.commit()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    c = openconnection.cursor()
    RROBIN_TABLE_PREFIX = 'round_robin_ratings_part'
    for i in range(0, numberofpartitions):
        c.execute("drop table if exists "+(RROBIN_TABLE_PREFIX + str(i))+";")
        c.execute("create table " +(RROBIN_TABLE_PREFIX + str(i))+ " (userid int, movieid int, rating float);")
        Query = "insert into " +(RROBIN_TABLE_PREFIX + str(i))+ "(userid, movieid, rating) (select userid, movieid, rating from (select userid, movieid, rating, row_number() over() as r from " +ratingstablename+ ") as bing where " +str(i)+ " = mod( bing.r"+"-1"+"," +str(numberofpartitions)+ ")) ; "                      
        c.execute(Query) 
    

    
    c.close()
    openconnection.commit()


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    c = openconnection.cursor()
    RROBIN_TABLE_PREFIX = 'round_robin_ratings_part'
    

    q = "insert into "+ratingstablename+"(userid, movieid, rating) values("+str(userid)+", "+str(itemid)+", "+str(rating)+");"
    c.execute(q)
    
    c.execute("select count(*) from "+ratingstablename+";")
    totalnumberofrows = c.fetchone()[0]
    
    q1 = "select count(*) from information_schema.tables where table_name like '%round_robin_ratings_part%';"
    c.execute(q1)
    numberofpartitions = c.fetchone()[0]
    print(numberofpartitions)
    i = ((totalnumberofrows - 1) % numberofpartitions)
    q2 = "insert into "+(RROBIN_TABLE_PREFIX+str(i))+"(userid, movieid, rating) values("+str(userid)+", "+str(itemid)+", "+str(rating)+");"
    c.execute(q2)
    
    
    c.close()
    openconnection.commit()


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    c = openconnection.cursor()
    RANGE_TABLE_PREFIX = 'range_ratings_part'
    q = "insert into "+ratingstablename+"(userid, movieid, rating) values("+str(userid)+", "+str(itemid)+", "+str(rating)+");"
    c.execute(q)

    q = "select count(*) from information_schema.tables where table_name like 'range_ratings_part%';"
    c.execute(q)
    numberofpartitions = c.fetchone()[0]
    print(numberofpartitions)
    diff = (5/numberofpartitions)
    minRating = 0
    maxRating = diff
    for i in range(0,numberofpartitions):
        if rating == 0:
            q1 = "insert into "+(RANGE_TABLE_PREFIX+str(i))+"(userid, movieid, rating) values("+str(userid)+", "+str(itemid)+", "+str(rating)+");"
            c.execute(q1)
        elif (rating > minRating) and (rating <= maxRating):
            q2 = "insert into "+(RANGE_TABLE_PREFIX+str(i))+"(userid, movieid, rating) values("+str(userid)+", "+str(itemid)+", "+str(rating)+");"
            c.execute(q2)
        else:
            minRating = maxRating
            maxRating = maxRating + diff
   
    c.close()
    openconnection.commit()


def rangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    c = openconnection.cursor()

    q = "select count(*) from information_schema.tables where table_name like '%range_ratings_part%';"
    c.execute(q)
    numberofRangeParts = c.fetchone()[0]

    print("The value of numberofRangeParts = ", numberofRangeParts)
    i = 0
    for i in range(0,numberofRangeParts):
        q1 = "select * from range_ratings_part"+str(i)+" where rating >= "+str(ratingMinValue)+" and rating <= "+str(ratingMaxValue)+";"
        c.execute(q1)
        p = c.fetchall()
        rtableName = "range_ratings_part"+str(i)
        for row in p:
            ruserid = row[0]
            rmovieid = row[1]
            rrating = row[2]
            rfinalString = rtableName+','+str(ruserid)+','+str(rmovieid)+','+str(rrating)
            textfile = open(outputPath,"a")
            textfile.write(rfinalString)
            textfile.write("\n")
    
   
    q2 = "select count(*) from information_schema.tables where table_name like '%round_robin_ratings_part%';"
    c.execute(q2)
    numberofRoundRobinParts = c.fetchone()[0]
    print("The number of round robin partitions are = ",numberofRoundRobinParts)
    j = 0
    for j in range(numberofRoundRobinParts):
        q2 = "select * from round_robin_ratings_part"+str(j)+" where rating >= "+str(ratingMinValue)+" and rating <= "+str(ratingMaxValue)+";"
        c.execute(q2)
        q = c.fetchall()
        rrtableName = "round_robin_ratings_part"+str(j)
        for row in q:
            rruserid = row[0]
            rrmovieid = row[1]
            rrrating = row[2]
            rrfinalString = rrtableName+','+str(rruserid)+','+str(rrmovieid)+','+str(rrrating)
            textfile = open(outputPath,"a")
            textfile.write(rrfinalString)
            textfile.write("\n")
    


def pointQuery(ratingValue, openconnection, outputPath):
    
    c = openconnection.cursor()
    
    
    q = "select count(*) from information_schema.tables where table_name like '%range_ratings_part%';"
    c.execute(q)
    numberofRangeParts = c.fetchone()[0]
    print("The value of numberofRangeParts = ", numberofRangeParts)
    i = 0
    for i in range(0,numberofRangeParts):
        q1 = "select * from range_ratings_part"+str(i)+" where rating = "+str(ratingValue)+";"
        c.execute(q1)
        r = c.fetchall()
        rtableName = "range_ratings_part"+str(i)
        for row in r:
            ruserid = row[0]
            rmovieid = row[1]
            rrating = row[2]
            rfinalString = rtableName+','+str(ruserid)+','+str(rmovieid)+','+str(rrating)
            textfile = open(outputPath,"a")
            textfile.write(rfinalString)
            textfile.write("\n")
    
  
    q2 = "select count(*) from information_schema.tables where table_name like '%round_robin_ratings_part%';"
    c.execute(q2)
    numberofRoundRobinParts = c.fetchone()[0]
    print("The number of round robin partitions are = ",numberofRoundRobinParts)
    j = 0
    for j in range(numberofRoundRobinParts):
        SQLquery2 = "select * from round_robin_ratings_part"+str(j)+" where rating = "+str(ratingValue)+";"
        c.execute(SQLquery2)
        s = c.fetchall()
        rrtableName = "round_robin_ratings_part"+str(j)
        for row in s:
            rruserid = row[0]
            rrmovieid = row[1]
            rrrating = row[2]
            rrfinalString = rrtableName+','+str(rruserid)+','+str(rrmovieid)+','+str(rrrating)
            textfile = open(outputPath,"a")
            textfile.write(rrfinalString)
            textfile.write("\n")       


def createDB(dbname='dds_assignment1'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
#createDB()
#connection = getOpenConnection()
#loadRatings("ratings","C:\\Users\\krajula\\Desktop\\DDS\\Assignment1\\Assignment1\\test_data1.txt", connection)
#rangePartition("ratings",5, connection)
#roundRobinPartition("ratings", 4, connection,"C:\\Users\\krajula\\Desktop\\DDS\\Assignment1\\Assignment1\\checking.txt")

#roundRobinInsert("ratings", 2, 222, 2.5, connection)
#rangeInsert("ratings", 3, 333, 3.5, connection)
#pointQuery(3,connection,"C:\\Users\\krajula\\Desktop\\DDS\\Assignment1\\Assignment1\\output.txt")
#rangeQuery(3,4,connection,"C:\\Users\\krajula\\Desktop\\DDS\\Assignment1\\Assignment1\\output.txt")

#loadRatings("f_ratings","C:\\Users\\krajula\\Desktop\\DDS\\Assignment1\\Assignment1\\ml-10m\\ml-10M100K\\ratings.dat", connection)
#C:\Users\krajula\Desktop\DDS\Assignment1\Assignment1\ml-10m\ml-10M100K